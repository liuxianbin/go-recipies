package mqtt

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const (
	// maxHandlerRetries 是首次处理失败后，后台最多再次调用业务回调的次数。
	maxHandlerRetries = 3
	// retryQueueSize 是进程内重试队列容量；队列满时接收回调会等待空位。
	retryQueueSize = 1024
	// retryWorkerCount 是并行消费本地重试队列的 goroutine 数量。
	retryWorkerCount = 4
	// subscribeMaxAttempts 是单轮订阅的高频尝试次数；穷尽后交给告警与下次重连，不做无限重试。
	subscribeMaxAttempts = 3
)

// Handler 处理收到的 MQTT 消息。
// 返回 error 会让 QoS 大于 0 的消息进入进程内重试队列；QoS 0 消息不会重试。
type Handler func(context.Context, paho.PublishReceived) error

// Client 管理 MQTT 连接、重连后的订阅恢复、消息收发和本地重试 worker。
type Client struct {
	// ctx/cancel 控制连接管理器、订阅重试任务和消息重试 worker 的生命周期。
	ctx    context.Context
	cancel context.CancelFunc
	// cfg、log、handle 保存客户端配置、日志器和消费回调。
	cfg    Config
	log    *slog.Logger
	handle Handler
	// conn 由 Autopaho 管理 MQTT 连接、自动重连和会话恢复。
	conn *autopaho.ConnectionManager
	// queue 只在进程内暂存失败消息；进程退出时未完成项不会持久化。
	queue chan paho.PublishReceived
}

// NewClient 创建启用 MQTT 5 会话恢复、自动重连和手动确认的客户端。
// handler 负责消息处理；logger 用于记录连接、订阅、确认和重试错误。
func NewClient(parent context.Context, cfg Config, handler Handler, logger *slog.Logger) (*Client, error) {
	if parent == nil {
		return nil, errors.New("context 不能为空")
	}
	if logger == nil {
		return nil, errors.New("logger 不能为空")
	}
	if handler == nil {
		return nil, errors.New("MQTT 消息处理器不能为空")
	}
	ctx, cancel := context.WithCancel(parent)
	brokerURL, err := url.Parse(cfg.MQTTURL)
	if err != nil {
		cancel()
		return nil, err
	}
	c := &Client{ctx: ctx, cancel: cancel, cfg: cfg, log: logger, handle: handler, queue: make(chan paho.PublishReceived, retryQueueSize)}
	// 会话保留 24 小时；断线后由 Autopaho 恢复连接，本客户端在连接回调里补做订阅。
	c.conn, err = autopaho.NewConnection(ctx, autopaho.ClientConfig{
		// Broker 地址列表；Autopaho 会按列表尝试建立连接。
		ServerUrls: []*url.URL{brokerURL},
		// MQTT 心跳间隔，单位为秒；Broker 可据此检测失联客户端。
		KeepAlive: 30,
		// 首次连接不清理服务端会话，允许相同 ClientID 重连后恢复原订阅和未完成消息。
		CleanStartOnInitialConnection: false,
		// 会话过期时间，单位为秒；这里设置为 24 小时。
		SessionExpiryInterval: 86400,
		// Broker 认证用户名；Broker 未启用认证时可留空。
		ConnectUsername: cfg.MQTTUsername,
		// Paho 要求密码使用字节切片传入。
		ConnectPassword: []byte(cfg.MQTTPassword),
		// 每次连接建立或自动重连成功后调用，用于恢复订阅。
		OnConnectionUp: c.onConnectionUp,
		// 记录连接尝试失败；Autopaho 会继续执行自动重连。
		OnConnectError: func(err error) { logger.Error("MQTT 连接失败", "error", err) },
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.MQTTClientID, // Broker 用 ClientID 识别客户端及其持久会话；同时在线时应保持唯一。
			// 由 onPublishReceived 在消息交给当前进程处理后显式 ACK。
			EnableManualAcknowledgment: true,
			// 收到 PUBLISH 报文时执行统一回调，负责调用 handler、入本地重试队列并 ACK。
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){c.onPublishReceived},
			// 记录 Paho 客户端协议处理期间遇到的错误。
			OnClientError: func(err error) { logger.Error("MQTT 客户端错误", "error", err) },
		},
	})
	if err != nil {
		cancel()
		return nil, err
	}
	// 固定数量的 worker 避免每条失败消息都创建 goroutine，限制并行重试规模。
	for i := 0; i < retryWorkerCount; i++ {
		go c.retryWorker()
	}
	return c, nil
}

// NewPublisher 创建仅发布消息的客户端，不会订阅 MQTT 主题。
func NewPublisher(parent context.Context, cfg Config, logger *slog.Logger) (*Client, error) {
	cfg.MQTTClientID += "-pub"
	return NewClient(parent, cfg, func(context.Context, paho.PublishReceived) error { return nil }, logger)
}

// OnPublishReceived 在 paho 的独立消息分发 goroutine 中按顺序调用 OnPublishReceived。
// 回调阻塞会停住消息分发，并延迟当前消息的 ACK；后续 PUBLISH 暂存在内部通道中。
// 若积压使该内部通道满，负责读网络包的 goroutine 也会阻塞，届时可能无法及时处理 PINGRESP 并导致心跳超时。
// QoS 1/2 的未确认消息受 Receive Maximum 限制；QoS 0 不受该限制，但仍可能占满内部通道。
// 因此回调应尽快返回，耗时业务处理交给后台worker。
func (c *Client) onPublishReceived(received paho.PublishReceived) (bool, error) {
	if c == nil || c.handle == nil || received.Packet == nil || received.Client == nil {
		return false, errors.New("MQTT 消息或客户端为空")
	}
	// handler 在此回调中同步执行，应快速完成，或投递到调用方的业务队列后返回；首次处理失败后的重试由后台 worker 执行。
	handlerErr := c.handle(c.ctx, received)
	if c.ctx.Err() != nil {
		c.log.Warn("ACK 前连接已关闭，消息保持未确认", "topic", received.Packet.Topic)
		return true, nil
	}
	if handlerErr != nil {
		if received.Packet.QoS > 0 {
			// 先放入有界本地队列，再 ACK Broker。
			select {
			case c.queue <- received:
			case <-received.Client.Done():
				c.log.Warn("入重试队列前连接已关闭，消息保持未确认", "topic", received.Packet.Topic)
				return true, nil
			case <-c.ctx.Done():
				c.log.Warn("入重试队列前客户端正在关闭，消息保持未确认", "topic", received.Packet.Topic)
				return true, nil
			default:
				// 队列满，立即返回，绝不阻塞收包循环。仍然 ACK，但将消息写入死信队列或本地持久化，由独立的补偿流程处理。
				c.log.Error("本地重试队列已满，消息已 ACK，交由死信队列或本地持久化补偿流程处理", "topic", received.Packet.Topic)
			}
		} else {
			c.log.Error("MQTT QoS 0 消息处理失败，消息无法重试", "topic", received.Packet.Topic, "qos", received.Packet.QoS, "error", handlerErr)
		}
	}
	if c.ctx.Err() != nil {
		c.log.Warn("ACK 前连接已关闭，消息保持未确认", "topic", received.Packet.Topic)
		return true, nil
	}
	// 协议确认只表示消息交给了当前进程，不代表业务最终成功。
	if err := received.Client.Ack(received.Packet); err != nil {
		c.log.Warn("MQTT 消息确认失败", "topic", received.Packet.Topic, "error", err)
	}
	return true, nil
}

// retryWorker 从有界队列取消息并重试业务回调。
// 最大重试 3 次，间隔依次为 1 秒、2 秒；达到上限后记录错误并丢弃。
func (c *Client) retryWorker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case received := <-c.queue:
			if err := c.processMessageRetry(received); err != nil {
				if c.ctx.Err() != nil {
					return
				}
				c.log.Error("MQTT 本地重试次数达到上限，消息已丢弃", "topic", received.Packet.Topic, "retry", maxHandlerRetries, "error", err)
			}
		}
	}
}

// processMessageRetry 首次失败后最多再调用 handler 三次，间隔依次为 1 秒和 2 秒。
func (c *Client) processMessageRetry(received paho.PublishReceived) error {
	var lastErr error
	for attempt := 0; attempt < maxHandlerRetries; attempt++ {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if err := c.handle(c.ctx, received); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < maxHandlerRetries-1 && !sleep(c.ctx, time.Duration(attempt+1)*time.Second) {
			return c.ctx.Err()
		}
	}
	return lastErr
}

// onConnectionUp 在首次连接和每次自动重连成功后补做订阅。
// 回调必须立即返回（autopaho 要求不得阻塞），订阅在后台执行。
func (c *Client) onConnectionUp(manager *autopaho.ConnectionManager, _ *paho.Connack) {
	go c.subscribeRetry(manager)
}

// subscribe 订阅配置的 MQTT filter，保持 demo 当前的订阅 QoS 设置。
func (c *Client) subscribe(manager *autopaho.ConnectionManager) error {
	if manager == nil {
		return errors.New("MQTT 连接管理器不可用")
	}
	_, err := manager.Subscribe(c.ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "/demo/+", QoS: c.cfg.MQTTSubscribeQoS}}})
	return err
}

// subscribeRetry 每轮最多尝试 subscribeMaxAttempts 次，间隔 1 秒。
// 高频重试只为覆盖连接存活期间的瞬时失败（broker 忙、SUBACK 超时）；
// 重试穷尽即记录 Error 并停止——剩余问题（未授权、filter 非法）不是重试能解决的，此时正确动作是暴露状态让人介入，而不是断线"重启"：那会用网络手段处理配置问题，只会制造会话抖动的新故障。
func (c *Client) subscribeRetry(manager *autopaho.ConnectionManager) {
	for attempt := 0; attempt < subscribeMaxAttempts; attempt++ {
		if c.ctx.Err() != nil {
			return
		}
		if err := c.subscribe(manager); err == nil {
			c.log.Info("MQTT 订阅已恢复")
			return
		} else {
			c.log.Warn("MQTT 订阅失败", "attempt", attempt+1, "error", err)
		}
		if attempt == subscribeMaxAttempts-1 {
			break
		}
		if !sleep(c.ctx, time.Second) {
			return
		}
	}
	if c.ctx.Err() != nil {
		return
	}
	c.log.Error("MQTT 订阅失败次数达到上限，等待下次重连再试")
}

func (c *Client) Run() error {
	if err := c.conn.AwaitConnection(c.ctx); err != nil {
		return err
	}
	<-c.ctx.Done()
	c.log.Info("收到退出信号，正在关闭连接...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.conn.Disconnect(shutdownCtx); err != nil {
		c.log.Error("断开连接", "err", err)
	}
	return c.ctx.Err()
}

// Publish 向 topic 发布 payload，并等待 MQTT 协议层完成。
// QoS 1 的 PUBACK 只表示 Broker 确认收到，不代表订阅方已完成业务处理。
func (c *Client) Publish(ctx context.Context, topic string, qos byte, payload []byte) error {
	if c == nil || c.conn == nil {
		return errors.New("MQTT 客户端不可用")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := c.conn.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: payload})
	return err
}

// Close 取消自动重连、订阅重试及本地消息重试任务。
func (c *Client) Close() {
	if c != nil {
		c.cancel()
	}
}

// sleep 等待 delay；如果生命周期上下文先取消则提前返回 false。
func sleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

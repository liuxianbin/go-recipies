package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"rabbitmq/config"
	"rabbitmq/message"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	// consumerCloseTimeout 是 Close 等待后台消费循环退出的最长时间。
	consumerCloseTimeout = 5 * time.Second
)

// Handler 处理一条已通过通用 envelope 校验的 RabbitMQ 消息原文。
// 返回错误时，消费者按网关策略记录错误并 ACK，不会将消息无限重新入队。
type Handler func(context.Context, message.Message) error

// Consumer 管理 RabbitMQ 连接、通道、消费回调、消息确认和断线重连。
type Consumer struct {
	// cfg、log、handle 保存连接参数、日志器和消息处理回调。
	cfg    config.Config
	log    *slog.Logger
	handle Handler
	// ctx/cancel 用于停止重连循环和当前消息消费。
	ctx    context.Context
	cancel context.CancelFunc
	// done 在后台重连/消费循环退出时关闭，供 Close 等待。
	done chan struct{}
}

// NewConsumer 校验参数并在后台启动 RabbitMQ 消费循环。
// 返回成功只表示后台循环已启动，不表示此时已经连上 Broker。
func NewConsumer(parent context.Context, cfg config.Config, handler Handler, logger *slog.Logger) (*Consumer, error) {
	if parent == nil || handler == nil || logger == nil {
		return nil, errors.New("上下文、消息处理函数和日志器不能为空")
	}
	ctx, cancel := context.WithCancel(parent)
	c := &Consumer{cfg: cfg, log: logger, handle: handler, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go c.run()
	return c, nil
}

// run 持续维护连接和通道：连接断开时重建连接，通道异常时优先复用当前连接。
func (c *Consumer) run() {
	defer close(c.done)
	attempt := 0
	for {
		// Context 已取消时不再尝试建立新的 RabbitMQ 连接。
		if c.ctx.Err() != nil {
			return
		}
		conn, err := amqp091.DialConfig(c.cfg.URL, amqp091.Config{
			SASL: []amqp091.Authentication{
				&amqp091.PlainAuth{Username: c.cfg.Username, Password: c.cfg.Password},
			},
		})
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			if permanentBrokerError(err) {
				c.log.Error("RabbitMQ 消费者遇到不可恢复的服务端错误，停止消费", "错误", err)
				return
			}
			// Broker 暂不可用时使用指数退避，避免高频拨号；退避从 1 秒增长至 30 秒。
			c.log.Error("RabbitMQ 消费者连接失败", "错误", err)
			if waitReconnect(c.ctx, attempt) != nil {
				return
			}
			attempt++
			continue
		}
		// 建连期间可能收到关闭信号，避免将新连接交给已停止的消费者。
		if c.ctx.Err() != nil {
			_ = conn.Close()
			return
		}
		attempt = 0
		c.log.Info("RabbitMQ 消费者连接已就绪")
		connClosed := conn.NotifyClose(make(chan *amqp091.Error, 1))
		var consumeDone chan error
		channelAttempt := 0
	Loop:
		for {
			consumeCtx, cancelConsume := context.WithCancel(c.ctx)
			consumeDone = make(chan error, 1)
			go func() {
				consumeDone <- c.consume(consumeCtx, conn)
			}()
			select {
			case err := <-connClosed:
				cancelConsume()
				if err != nil {
					c.log.Error("RabbitMQ 连接已关闭，准备重连", "错误", err)
				} else {
					c.log.Warn("RabbitMQ 连接关闭，准备重连")
				}
				break Loop
			case err := <-consumeDone:
				cancelConsume()
				consumeDone = nil
				if c.ctx.Err() != nil {
					break Loop
				}
				if err != nil {
					c.log.Warn("RabbitMQ 通道消费结束，准备重建", "错误", err)
					if permanentBrokerError(err) {
						break Loop
					}
				}
				// 通道创建或消费失败时退避；若连接也已关闭，则重建连接。
				if waitErr := waitReconnectOrConnClose(c.ctx, channelAttempt, connClosed); waitErr != nil {
					if c.ctx.Err() == nil {
						c.log.Warn("RabbitMQ 连接已关闭，准备重连", "错误", waitErr)
					}
					break Loop
				}
				channelAttempt++
			case <-c.ctx.Done():
				cancelConsume()
				break Loop
			}
		}
		_ = conn.Close()
		if consumeDone != nil {
			<-consumeDone
		}
		if c.ctx.Err() != nil {
			return
		}
	}
}

// consume 在独立通道上声明拓扑并接收消息；出错时返回给 run 决定是否重试。
func (c *Consumer) consume(ctx context.Context, conn *amqp091.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("创建通道失败：%w", err)
	}
	defer ch.Close()
	// 交换机、队列和绑定关系都声明为持久化拓扑，消费者重启后仍可继续使用。
	if err := ch.ExchangeDeclare("demo-exchange", amqp091.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("声明交换机失败：%w", err)
	}
	if _, err := ch.QueueDeclare("demo-queue", true, false, false, false, nil); err != nil {
		return fmt.Errorf("声明队列失败：%w", err)
	}
	if err := ch.QueueBind("demo-queue", "demo-key", "demo-exchange", false, nil); err != nil {
		return fmt.Errorf("绑定队列失败：%w", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("设置预取数量失败：%w", err)
	}
	// autoAck=false 启用手动确认；prefetch=1 保证同一消费者最多有一条未确认消息。
	msgs, err := ch.Consume("demo-queue", "go-demo-consumer", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("注册消费者失败：%w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp091.Error, 1))
	c.log.Info("RabbitMQ 消费者已就绪")
	for {
		select {
		case <-ctx.Done():
			// 关闭时不确认处理中消息；通道或连接关闭后，Broker 会重新投递该消息。
			return ctx.Err()
		case err := <-closed:
			if err == nil {
				return errors.New("RabbitMQ 通道已关闭")
			}
			return fmt.Errorf("RabbitMQ 通道关闭：%w", err)
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("RabbitMQ 消息通道已关闭")
			}
			var m message.Message
			if m, err = message.DecodeMessage(msg.Body); err != nil {
				// 格式错误通常不能靠重复投递修复，因此拒绝且不 requeue，避免毒消息循环。
				c.log.Error("RabbitMQ 消息格式错误，拒绝且不重新入队", "error", err)
				if nackErr := msg.Nack(false, false); nackErr != nil {
					c.log.Warn("RabbitMQ 消息 NACK 失败", "error", nackErr)
				}
				continue
			}
			if err := c.handle(ctx, m); err != nil {
				if ctx.Err() != nil {
					// 关闭期间处理失败时不 ACK；连接关闭后由 Broker 重新投递在途消息。
					return ctx.Err()
				}
				// 与网关策略一致：运行期间处理失败也 ACK，避免业务错误造成无限重投。
				c.log.Error("RabbitMQ 消息处理失败，按网关策略确认并丢弃", "error", err)
				if ackErr := msg.Ack(false); ackErr != nil {
					c.log.Warn("RabbitMQ 消息 ACK 失败", "error", ackErr)
				}
				continue
			}
			if err := msg.Ack(false); err != nil {
				c.log.Warn("RabbitMQ 消息 ACK 失败", "error", err)
			}
		}
	}
}

// Close 取消消费和重连，并最多等待 5 秒让后台循环释放当前资源。
func (c *Consumer) Close() {
	if c == nil {
		return
	}
	c.cancel()
	timer := time.NewTimer(consumerCloseTimeout)
	defer timer.Stop()
	select {
	case <-c.done:
	case <-timer.C:
		c.log.Warn("等待 RabbitMQ consumer 关闭超时")
	}
}

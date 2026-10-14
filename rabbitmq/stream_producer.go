package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"rabbitmq/config"
	"sync"
	"sync/atomic"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// StreamProducer 持有 Stream 环境和可靠 Super Stream 生产者，供长生命周期服务重复发布消息。
type StreamProducer struct {
	environment      *stream.Environment
	producer         *ha.ReliableSuperStreamProducer
	config           config.Config
	logger           *slog.Logger
	nextPublishingID atomic.Int64
	closeOnce        sync.Once
}

// NewStreamProducer 声明 Super Stream 并创建可靠生产者。
// 分区连接异常时由 ReliableSuperStreamProducer 负责恢复；Stream 上限由配置限制。
func NewStreamProducer(ctx context.Context, cfg config.Config, logger *slog.Logger) (*StreamProducer, error) {
	if logger == nil {
		return nil, errors.New("日志器不能为空")
	}
	if ctx == nil {
		return nil, errors.New("上下文不能为空")
	}
	if cfg.StreamPartitions <= 0 || cfg.StreamMaxLengthGB <= 0 {
		return nil, errors.New("Stream 分区数和最大保留容量必须大于 0")
	}
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetHost(cfg.StreamHost).SetPort(cfg.StreamPort).SetUser(cfg.StreamUsername).SetPassword(cfg.StreamPassword))
	if err != nil {
		return nil, err
	}
	result := &StreamProducer{environment: env, config: cfg, logger: logger}
	created := false
	defer func() {
		if !created {
			if result.producer != nil {
				_ = result.producer.Close()
			}
			_ = env.Close()
		}
	}()
	if err := env.DeclareSuperStream(cfg.StreamName,
		stream.NewPartitionsOptions(cfg.StreamPartitions).
			SetMaxLengthBytes(stream.ByteCapacity{}.GB(int64(cfg.StreamMaxLengthGB)))); err != nil {
		return nil, err
	}
	result.producer, err = ha.NewReliableSuperStreamProducer(env, cfg.StreamName,
		stream.NewSuperStreamProducerOptions(stream.NewHashRoutingStrategy(func(msg message.StreamMessage) string {
			return msg.GetMessageProperties().MessageID.(string)
		})).SetClientProvidedName("go-demo-stream-producer"),
		func(confirms []*stream.PartitionPublishConfirm) {
			// 在这里处理异步发布确认；调用 Publish 成功只表示客户端接受了发送请求。
			for _, confirm := range confirms {
				for _, status := range confirm.ConfirmationStatus {
					if status.IsConfirmed() {
						continue
					}
					logger.Error("Stream 消息未收到确认", "发布编号", status.GetPublishingId(), "错误", status.GetError())
				}
			}
		})
	if err != nil {
		return nil, err
	}
	created = true
	logger.Info("RabbitMQ Super Stream 生产者已就绪", "流名称", cfg.StreamName, "分区数", cfg.StreamPartitions)
	return result, nil
}

// Publish 使用 key 做哈希路由并提交消息；返回值是本进程内递增的 publishing ID。
// 成功返回只表示客户端接受了发送请求，Broker 的异步结果通过确认回调报告。
func (p *StreamProducer) Publish(body []byte, key string) (int64, error) {
	if p == nil || p.producer == nil {
		return 0, errors.New("RabbitMQ Stream 生产者不可用")
	}
	if key == "" {
		return 0, errors.New("Stream 路由 key 不能为空")
	}
	id := p.nextPublishingID.Add(1)
	msg := amqp.NewMessage(body)
	msg.Properties = &amqp.MessageProperties{MessageID: key}
	msg.SetPublishingId(id)
	if err := p.producer.Send(msg); err != nil {
		return 0, err
	}
	return id, nil
}

// Close 先关闭可靠生产者，再关闭 Stream 环境；重复调用不会重复释放资源。
func (p *StreamProducer) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		if p.producer != nil {
			_ = p.producer.Close()
		}
		if p.environment != nil {
			_ = p.environment.Close()
		}
	})
}

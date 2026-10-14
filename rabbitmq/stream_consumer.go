package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"rabbitmq/config"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// NewStreamConsume consumes all Super Stream partitions, resuming from saved offsets.
// ReliableSuperStreamConsumer 在进程运行期间恢复异常断开的分区；ctx 取消时关闭 consumer 和 environment。
func NewStreamConsume(ctx context.Context, cfg config.Config, logger *slog.Logger, handle func([]byte)) error {
	if ctx == nil || logger == nil || handle == nil {
		return errors.New("context、logger 和 handler 不能为空")
	}
	if cfg.StreamPartitions <= 0 {
		return errors.New("Stream 分区数必须大于 0")
	}
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetHost(cfg.StreamHost).SetPort(cfg.StreamPort).SetUser(cfg.StreamUsername).SetPassword(cfg.StreamPassword))
	if err != nil {
		return err
	}
	defer env.Close()
	consumerName := "go-demo-stream-consumer"
	sac := stream.NewSingleActiveConsumer(func(partition string, isActive bool) stream.OffsetSpecification {
		lastOffset, err := env.QueryOffset(consumerName, partition)
		switch {
		case err == nil:
			return stream.OffsetSpecification{}.Offset(lastOffset + 1)
		case errors.Is(err, stream.OffsetNotFoundError):
			// 该分区首次消费
			return stream.OffsetSpecification{}.First()
		default:
			// ConsumerUpdate 不能返回 error；记录查询故障。
			// 查询失败时从头消费，避免跳过保留消息；可能重放已有消息。
			logger.Error("query partition offset failed",
				"partition", partition, "error", err)
			return stream.OffsetSpecification{}.First()
		}
	})
	strategy := stream.NewAutoCommitStrategy().SetCountBeforeStorage(1000).SetFlushInterval(5 * time.Second)
	consumer, err := ha.NewReliableSuperStreamConsumer(env, cfg.StreamName,
		func(messageCtx stream.ConsumerContext, msg *amqp.Message) {
			// Stream 客户端将消息数据拆成若干段，这里拼回原始 JSON body 后交给回调。
			logger.Info("Stream 收到消息", "partition", messageCtx.Consumer.GetStreamName(), "offset", messageCtx.Consumer.GetOffset())
			//partition := messageCtx.Consumer.GetStreamName()
			//offset := messageCtx.Consumer.GetOffset()
			//if err := env.StoreOffset(consumerName, partition, offset); err != nil {
			//	logger.Error("failed to store Stream offset", "partition", messageCtx.Consumer.GetStreamName(), "offset", messageCtx.Consumer.GetOffset(), "error", err)
			//	return
			//}
			handle(bytes.Join(msg.Data, nil))
		}, stream.NewSuperStreamConsumerOptions().
			SetConsumerName("go-demo-stream-consumer").
			SetSingleActiveConsumer(sac).
			//SetManualCommit().
			SetAutoCommit(strategy).
			SetOffset(stream.OffsetSpecification{}.First()))
	if err != nil {
		return err
	}
	<-ctx.Done()
	// 先停止消费端，再由 defer 关闭底层环境连接。
	return consumer.Close()
}

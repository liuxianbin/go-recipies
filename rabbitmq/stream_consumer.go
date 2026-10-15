package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"rabbitmq/config"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// NewStreamConsume consumes all Super Stream partitions, resuming from saved offsets.
// ReliableSuperStreamConsumer 在进程运行期间恢复异常断开的分区；ctx 取消时关闭 consumer 和 environment。
func NewStreamConsume(ctx context.Context, cfg config.Config, logger *slog.Logger, handle func([]byte) error) error {
	// 这些依赖缺失时无法安全启动消费，提前返回配置错误。
	if ctx == nil || logger == nil || handle == nil {
		return errors.New("context、logger 和 handler 不能为空")
	}
	if strings.TrimSpace(cfg.StreamName) == "" {
		return errors.New("StreamName 不能为空")
	}
	// Environment 持有 Stream 连接；函数退出时统一关闭。
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetHost(cfg.StreamHost).SetPort(cfg.StreamPort).SetUser(cfg.StreamUsername).SetPassword(cfg.StreamPassword))
	if err != nil {
		return err
	}
	defer env.Close()
	// consumerName 是 offset 的持久化键，重启后必须保持稳定。
	consumerName := "go-demo-stream-consumer"
	// 新分区从头消费；已有分区从已保存 offset 的下一条开始，避免重复读取已提交消息。
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
			logger.Error("查询分区 offset 失败",
				"partition", partition, "error", err)
			return stream.OffsetSpecification{}.First()
		}
	})
	// 每个分区独立累计和提交 offset，不能把不同分区的消息合并成一个批次。
	type partitionBatch struct {
		// handler 和定时提交协程会同时访问批次状态，因此需要按分区加锁。
		sync.Mutex
		pending           int       // 上次 StoreOffset 未报错后，已计入批次的消息数
		lastHandledOffset int64     // 本批次最后一条计入提交的消息 offset
		lastCommitAttempt time.Time // 上次提交失败时间，用于控制重试频率
	}
	// sync.Map 让每个分区都能取得自己的批次状态。
	var partitionBatches sync.Map
	// 提交该分区当前批次最后一条计入提交的消息 offset。
	storeBatchOffset := func(partition string, batch *partitionBatch) {
		if batch.pending == 0 {
			return
		}
		// 保存最后一条计入提交的消息 offset；SAC 恢复时从该 offset + 1 开始。
		offset := batch.lastHandledOffset
		// StoreOffset 返回 nil 只表示请求写入成功，不代表 broker 已确认保存。
		if err := env.StoreOffset(consumerName, partition, offset); err != nil {
			// 保留批次状态，后续继续重试提交。
			batch.lastCommitAttempt = time.Now()
			logger.Error("保存 Stream offset 失败", "partition", partition, "offset", offset, "error", err)
			return
		}
		batch.pending = 0
		// StoreOffset 未报错后清除重试时间；下一批达到 500 条时可立即尝试提交。
		batch.lastCommitAttempt = time.Time{}
	}
	// 标记开始关停，避免未完成的 handler 在最终刷新后再推进 offset。
	var closing atomic.Bool
	consumer, err := ha.NewReliableSuperStreamConsumer(env, cfg.StreamName,
		func(messageCtx stream.ConsumerContext, msg *amqp.Message) {
			// 关停开始后新到的回调不再开始处理；offset 未推进，消息会重放。
			if closing.Load() {
				return
			}
			// Stream 客户端将消息数据拆成若干段，这里拼回原始消息 body 后交给回调。
			partition := messageCtx.Consumer.GetStreamName()
			offset := messageCtx.Consumer.GetOffset()
			// 同一分区复用同一个批次状态，不同分区分别提交 offset。
			batchValue, _ := partitionBatches.LoadOrStore(partition, &partitionBatch{})
			batch := batchValue.(*partitionBatch)
			body := bytes.Join(msg.Data, nil)
			logger.Debug("Stream 收到消息", "partition", partition, "offset", offset)
			if err := processWithRetry(handle, body); err != nil {
				logger.Error("Stream 消息处理重试耗尽", "partition", partition, "offset", offset, "attempts", handlerMaxAttempts, "error", err)
				// TODO: 持久化失败消息后继续消费；用 partition + offset 幂等，后续提交可越过它。
				// 目前只是占位注释：代码仍会继续计入提交批次；接入存储前，失败消息可能随 offset 推进而丢失。
			}
			batch.Lock()
			defer batch.Unlock()
			// 关停期间未完成的 handler 不推进 offset，重启后会重新消费这条消息。
			if closing.Load() {
				return
			}
			batch.lastHandledOffset = offset
			batch.pending++
			// 满 500 条立即尝试提交；定时器负责刷新未满批次。
			if batch.pending >= manualCommitCount &&
				(batch.lastCommitAttempt.IsZero() || time.Since(batch.lastCommitAttempt) >= manualCommitInterval) {
				// 上次提交失败后，避免每条新消息都立刻重复提交；定时器也遵守相同间隔。
				storeBatchOffset(partition, batch)
			}
		}, stream.NewSuperStreamConsumerOptions().
			SetConsumerName(consumerName).
			SetSingleActiveConsumer(sac).
			// 关闭客户端自动提交，由上面的批量逻辑统一推进 offset；失败消息应先持久化。
			SetManualCommit().
			SetOffset(stream.OffsetSpecification{}.First()))
	if err != nil {
		// consumer 未创建成功，不进入等待和提交循环；defer 会关闭 Environment。
		return err
	}
	// 定时提交独立运行；ctx 取消后主协程会等待它退出，再做最后一次刷新。
	flushDone := make(chan struct{})
	go func() {
		// 用于通知主协程：定时提交循环已退出，可以安全地继续关闭 consumer。
		defer close(flushDone)
		// 定时刷新未满 500 条的批次，避免低流量分区长期不提交。
		ticker := time.NewTicker(manualCommitInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				partitionBatches.Range(func(key, value any) bool {
					partition := key.(string)
					batch := value.(*partitionBatch)
					batch.Lock()
					if batch.lastCommitAttempt.IsZero() || time.Since(batch.lastCommitAttempt) >= manualCommitInterval {
						// 没有提交失败冷却时刷新；提交失败时等满 5 秒再重试。
						storeBatchOffset(partition, batch)
					}
					batch.Unlock()
					return true
				})
			}
		}
	}()
	<-ctx.Done()
	// 先封住 handler 的 offset 更新，再停定时器并刷新已完成的消息。
	closing.Store(true)
	// 先停止定时提交，避免 consumer 关闭期间定时器仍尝试提交。
	<-flushDone
	// 先刷新尾批；关停时仍在处理的消息不推进 offset，重启后会重放。
	partitionBatches.Range(func(key, value any) bool {
		batch := value.(*partitionBatch)
		batch.Lock()
		storeBatchOffset(key.(string), batch)
		batch.Unlock()
		return true
	})
	return consumer.Close()
}

const (
	// 首次调用之外的业务 handler 重试次数。
	handlerRetryCount = 3
	// 每次重试前等待，避免失败时立即连续调用业务 handler。
	handlerRetryDelay  = 500 * time.Millisecond
	handlerMaxAttempts = handlerRetryCount + 1
	// 每个分区累计到此条数时尝试提交；定时器也会刷新不足此数量的批次。
	manualCommitCount = 500
	// 未满批次的刷新周期，也是提交失败后的最短重试间隔。
	manualCommitInterval = 5 * time.Second
)

// 最多调用 handler 4 次：首次处理 1 次，失败后再重试 3 次。
func processWithRetry(handle func([]byte) error, body []byte) error {
	var err error
	for attempt := 0; attempt < handlerMaxAttempts; attempt++ {
		if err = handle(body); err == nil {
			return nil
		}
		if attempt < handlerRetryCount {
			time.Sleep(handlerRetryDelay)
		}
	}
	return err
}

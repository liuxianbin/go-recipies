package rabbitmq

import (
	"context"
	"errors"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

const (
	// initialReconnectDelay 是第一次重连前的等待时间。
	initialReconnectDelay = time.Second
	// maxReconnectDelay 是重连退避的最大等待时间。
	maxReconnectDelay = 30 * time.Second
)

// waitReconnect 等待指数退避时间，避免 RabbitMQ 故障时频繁重连。
func waitReconnect(ctx context.Context, attempt int) error {
	delay := initialReconnectDelay
	for i := 0; i < attempt && delay < maxReconnectDelay; i++ {
		delay *= 2
	}
	if delay > maxReconnectDelay {
		delay = maxReconnectDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// waitReconnectOrConnClose 等待退避时间、上下文取消或连接关闭通知，以便及时停止无效重试。
func waitReconnectOrConnClose(ctx context.Context, attempt int, connClose <-chan *amqp091.Error) error {
	delay := initialReconnectDelay
	for i := 0; i < attempt && delay < maxReconnectDelay; i++ {
		delay *= 2
	}
	if delay > maxReconnectDelay {
		delay = maxReconnectDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case err := <-connClose:
		if err == nil {
			return errors.New("RabbitMQ 连接已关闭")
		}
		return err
	}
}

// permanentBrokerError 判断错误是否属于认证或协议问题；这类错误需要修正配置或拓扑后才能恢复。
func permanentBrokerError(err error) bool {
	var brokerErr *amqp091.Error
	if !errors.As(err, &brokerErr) {
		return false
	}
	switch brokerErr.Code {
	case amqp091.AccessRefused, amqp091.PreconditionFailed, amqp091.NotAllowed, amqp091.NotImplemented:
		return true
	default:
		return false
	}
}

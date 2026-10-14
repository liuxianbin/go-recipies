package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"rabbitmq/config"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Publish 持续尝试发布，直到收到 Broker 确认或 ctx 取消。
// 每次尝试都会新建连接；连接、发布或确认失败后按 1 至 30 秒指数退避。
func Publish(ctx context.Context, cfg config.Config, body []byte) error {
	if ctx == nil {
		return errors.New("上下文不能为空")
	}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		err := publishOnce(ctx, cfg, body)
		if err == nil {
			return nil
		}
		if permanentBrokerError(err) {
			return err
		}
		fmt.Printf("RabbitMQ 消息发布失败，准备重试：%v\n", err)
		if !waitPublishRetry(ctx, backoff(attempt)) {
			break
		}
	}
	return ctx.Err()
}

// publishOnce 执行一次发布：声明直连交换机、启用确认、发送持久消息并等待 ACK。
func publishOnce(ctx context.Context, cfg config.Config, body []byte) error {
	conn, err := amqp091.DialConfig(cfg.URL, amqp091.Config{
		SASL: []amqp091.Authentication{
			&amqp091.PlainAuth{Username: cfg.Username, Password: cfg.Password},
		},
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		return err
	}
	return publishAndConfirm(ctx, ch, body)
}

// publishAndConfirm 先注册 Broker 通知，再发布消息并等待发布结果。
func publishAndConfirm(ctx context.Context, ch *amqp091.Channel, body []byte) error {
	// 先注册通知通道，避免发布确认或不可路由退回消息早于监听器注册。
	confirms := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp091.Return, 1))
	if err := ch.PublishWithContext(ctx, "demo-exchange", "demo-key", true, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Body:         body,
	}); err != nil {
		return err
	}
	// mandatory=true 时不可路由消息会被退回；Publisher ACK 只表示 Broker 接收，不表示消费者完成。
	return waitPublishResult(ctx, returns, confirms)
}

// waitPublishResult 等待 mandatory 消息退回通知或 Publisher 确认。
func waitPublishResult(ctx context.Context, returns <-chan amqp091.Return, confirms <-chan amqp091.Confirmation) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case returned := <-returns:
		return fmt.Errorf("消息无法路由并被退回（%d %s）", returned.ReplyCode, returned.ReplyText)
	case confirmation, ok := <-confirms:
		if !ok || !confirmation.Ack {
			return errors.New("消息未收到 Broker 确认")
		}
		// AMQP 规定退回通知先于对应的发布确认；收到确认后再检查一次退回通知通道。
		select {
		case returned := <-returns:
			return fmt.Errorf("消息无法路由并被退回（%d %s）", returned.ReplyCode, returned.ReplyText)
		default:
			return nil
		}
	}
}

// backoff 计算指数退避时长，最长不超过 30 秒。
func backoff(attempt int) time.Duration {
	delay := time.Second
	for i := 0; i < attempt && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

// waitPublishRetry 可被 ctx 取消的重试等待，避免关闭时还在睡眠期间等待 Broker。
func waitPublishRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

package mqtt

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// TestNewPublisher 演示如何向 Broker 发布一条消息。
// 使用前请设置 MQTT_URL 等 MQTT_* 环境变量。
func TestNewPublisher(t *testing.T) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	client, err := NewPublisher(ctx, Load(), logger)
	if err != nil {
		logger.Error("创建 MQTT 发布客户端失败", "error", err)
		return
	}
	defer client.Close()

	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		if err := client.Run(); err != nil && ctx.Err() == nil {
			logger.Error("运行 MQTT 发布客户端失败", "error", err)
			stop()
		}
	}()
	if err := client.conn.AwaitConnection(publishCtx); err != nil {
		t.Fatalf("等待 MQTT 连接失败: %v", err)
	}
	// Publish 会等待 MQTT 协议交互完成。超时也限制了 Broker 不可用时的初始连接等待时间。
	if err := client.Publish(publishCtx, "/demo/example", 1, []byte("hello from MQTT publisher")); err != nil {
		logger.Error("发布 MQTT 消息失败", "error", err)
		return
	}
	logger.Info("MQTT 消息已发布", "topic", "/demo/example")

	// 保持连接直到按下 Ctrl+C，让 Run 执行断开连接的清理流程。
	<-ctx.Done()
}

// TestNewClient 演示如何接收已配置订阅主题（/demo/+）上的消息。
// 使用前请设置 MQTT_URL 等 MQTT_* 环境变量。
func TestNewClient(t *testing.T) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	client, err := NewClient(ctx, Load(), func(_ context.Context, received paho.PublishReceived) error {
		logger.Info("收到 MQTT 消息",
			"topic", received.Packet.Topic,
			"qos", received.Packet.QoS,
			"payload", string(received.Packet.Payload),
		)
		return nil
	}, logger)
	if err != nil {
		logger.Error("创建 MQTT 订阅客户端失败", "error", err)
		return
	}
	defer client.Close()

	if err := client.Run(); err != nil && ctx.Err() == nil {
		logger.Error("运行 MQTT 订阅客户端失败", "error", err)
	}
}

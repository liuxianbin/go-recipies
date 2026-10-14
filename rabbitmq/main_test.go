package rabbitmq

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"rabbitmq/config"
	"rabbitmq/message"
	"syscall"
	"testing"
)

func TestConsumer(t *testing.T) {
	// 使用 JSON 结构化日志，便于 Docker、Kubernetes 或日志平台按字段检索。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("读取配置失败", "错误", err)
		os.Exit(1)
	}
	// 创建根 Context：收到进程终止信号时，所有后台重连、消费和发布任务都应停止。
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	commandConsumer, err := NewConsumer(ctx, cfg, func(ctx context.Context, m message.Message) error {
		slog.Info("处理消息", "消息", m)
		return nil
	}, logger)
	if err != nil {
		logger.Error("创建命令消费者失败", "错误", err)
		os.Exit(1)
	}
	// 退出时停止消费循环并关闭当前 AMQP 连接；未确认的命令由 RabbitMQ 重新投递。
	defer commandConsumer.Close()
	<-ctx.Done()
}

func TestPublish(t *testing.T) {
	// 使用 JSON 结构化日志，便于 Docker、Kubernetes 或日志平台按字段检索。
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("读取配置失败", "错误", err)
		os.Exit(1)
	}
	// 创建根 Context：收到进程终止信号时，所有后台重连、消费和发布任务都应停止。
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	data, err := message.Wrap("1001", "hello world")
	if err != nil {
		logger.Error("封装消息失败", "错误", err)
		return
	}
	err = Publish(ctx, cfg, data)
	if err != nil {
		logger.Error("发布消息失败", "错误", err)
		return
	}
}

func TestStreamPublish(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("读取配置失败：%v", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	producer, err := NewStreamProducer(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("创建 Stream 生产者失败：%v", err)
	}
	defer producer.Close()
	body, err := message.Wrap("1001", "hello Stream")
	if err != nil {
		t.Fatalf("封装 Stream 消息失败：%v", err)
	}
	id, err := producer.Publish(body, "stream-demo-key")
	if err != nil {
		t.Fatalf("发布 Stream 消息失败：%v", err)
	}
	logger.Info("Stream 消息已提交", "发布编号", id)
	<-ctx.Done()
}

func TestStreamConsume(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("读取配置失败：%v", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := NewStreamConsume(ctx, cfg, logger, func(body []byte) error {
		logger.Info("Stream 收到消息", "消息体", string(body))
		return nil
	}); err != nil {
		t.Fatalf("消费 Stream 消息失败：%v", err)
	}
}

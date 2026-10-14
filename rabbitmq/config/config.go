package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	URL                string `mapstructure:"url"`                  // RabbitMQ AMQP 服务地址。
	Username           string `mapstructure:"username"`             // RabbitMQ AMQP 连接用户名。
	Password           string `mapstructure:"password"`             // RabbitMQ AMQP 连接密码。
	StreamHost         string `mapstructure:"stream-host"`          // RabbitMQ Stream 服务主机地址。
	StreamPort         int    `mapstructure:"stream-port"`          // RabbitMQ Stream 服务端口。
	StreamName         string `mapstructure:"stream-name"`          // 定位事件 Super Stream 名称。
	StreamConsumerName string `mapstructure:"stream-consumer-name"` // 用于保存各分区消费 offset 的稳定消费者名。
	StreamPartitions   int    `mapstructure:"stream-partitions"`    // Super Stream 分区数量。
	StreamUsername     string `mapstructure:"stream-username"`      // RabbitMQ Stream 连接用户名。
	StreamPassword     string `mapstructure:"stream-password"`      // RabbitMQ Stream 连接密码。
	StreamMaxLengthGB  int    `mapstructure:"stream-max-length-gb"` // Stream 分区流最大保留容量，单位为 GB。
}

// Load 从 YAML 配置文件读取配置；默认读取 configs/config.yaml，也可通过 CONFIG_FILE 指定其他文件。
func Load() (Config, error) {
	configPath := os.Getenv("CONFIG_FILE")
	if configPath == "" {
		configPath = "config/config.yaml"
	}
	reader := viper.New()
	reader.SetConfigFile(configPath)
	reader.SetConfigType("yaml")
	if err := reader.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("读取配置文件 %q 失败：%w", configPath, err)
	}
	var cfg Config
	if err := reader.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("解析配置文件 %q 失败：%w", configPath, err)
	}
	return cfg, nil
}

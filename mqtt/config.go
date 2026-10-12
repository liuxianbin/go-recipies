package mqtt

import (
	"os"
	"strconv"
)

// Config 保存三个消息中间件连接和收发所需的配置。
// 配置从环境变量读取；未设置时使用本机 Compose 服务对应的演示默认值。
type Config struct {
	// MQTTURL 是 MQTT 5 Broker 的连接地址，例如 mqtt://127.0.0.1:1883。
	MQTTURL string
	// MQTTUsername 和 MQTTPassword 是 MQTT Broker 的可选认证凭据。
	MQTTUsername string
	MQTTPassword string
	// MQTTClientID 是 Broker 用于识别 MQTT 客户端和恢复会话的 ID。
	MQTTClientID string
	// MQTTSubscribeQoS 是订阅请求的最大 QoS；默认 1。
	MQTTSubscribeQoS byte
}

// Load 读取支持的环境变量，并为本地演示填充默认值。
func Load() Config {
	return Config{
		MQTTURL:          env("MQTT_URL", "mqtt://192.168.5.81:1883"),
		MQTTUsername:     os.Getenv("MQTT_USERNAME"),
		MQTTPassword:     os.Getenv("MQTT_PASSWORD"),
		MQTTClientID:     env("MQTT_CLIENT_ID", "go-demo-client"),
		MQTTSubscribeQoS: envQoS("MQTT_SUBSCRIBE_QOS", 1),
	}
}

// env 返回非空环境变量；变量未设置或为空时返回 fallback。
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// envQoS 只接受 MQTT 支持的 QoS 0 或 1；其他输入时回退到默认值。
func envQoS(key string, fallback byte) byte {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 0 || value > 1 {
		return fallback
	}
	return byte(value)
}

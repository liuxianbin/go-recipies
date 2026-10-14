package message

import (
	"encoding/json"
)

// Message 是 RabbitMQ 消息体使用的通用命令与内容结构。
type Message struct {
	Cmd     string `json:"cmd"`
	Content string `json:"content"`
}

// Wrap 将命令和内容编码为 JSON 消息体。
func Wrap(cmd string, content string) ([]byte, error) {
	return json.Marshal(Message{Cmd: cmd, Content: content})
}

// DecodeMessage 将 JSON 消息体解析为 Message。
func DecodeMessage(body []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	constant "github.com/li1553770945/personal-aichat-service/biz/constant"
	aichat "github.com/li1553770945/personal-aichat-service/kitex_gen/aichat"
	base "github.com/li1553770945/personal-aichat-service/kitex_gen/base"
)

const difyAPIURL = "https://api.dify.ai/v1/chat-messages"

// DifyRequest represents the request body for Dify API
type DifyRequest struct {
	Query          string                 `json:"query"`
	Inputs         map[string]interface{} `json:"inputs,omitempty"`
	ResponseMode   string                 `json:"response_mode"`
	User           string                 `json:"user"`
	ConversationID string                 `json:"conversation_id,omitempty"`
	Files          []interface{}          `json:"files,omitempty"`
}

// DifyStreamResponse represents a single SSE event from Dify API
type DifyStreamResponse struct {
	Event              string          `json:"event"`
	TaskID             string          `json:"task_id,omitempty"`
	ID                 string          `json:"id,omitempty"`
	MessageID          string          `json:"message_id,omitempty"`
	ConversationID     string          `json:"conversation_id,omitempty"`
	Answer             string          `json:"answer,omitempty"`
	CreatedAt          int64           `json:"created_at,omitempty"`
	Status             int             `json:"status,omitempty"`
	Code               string          `json:"code,omitempty"`
	Message            string          `json:"message,omitempty"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
	Usage              json.RawMessage `json:"usage,omitempty"`
	RetrieverResources json.RawMessage `json:"retriever_resources,omitempty"`
}

// SendMessage 发送消息到dify api并流式返回响应
func (s *AIChatService) SendMessage(ctx context.Context, req *aichat.SendMessageReq, stream aichat.AIChatService_SendMessageServer) error {
	// 请求参数
	difyReq := DifyRequest{
		Query:        req.Message, // Use Message field from IDL
		Inputs:       make(map[string]interface{}),
		ResponseMode: "streaming",    // Use streaming mode
		User:         "default_user", // Default user ID for now
	}

	// ConversationID is not in the current IDL, but can be added later
	// if req.ConversationID != "" {
	// 	difyReq.ConversationID = req.ConversationID
	// }

	// Marshal request body
	reqBody, err := json.Marshal(difyReq)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, "POST", difyAPIURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.Config.AIChatConfig.APIKey))

	// Send request
	client := &http.Client{Timeout: 300 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("发送请求到dify api失败: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dify api返回状态码: %d", resp.StatusCode)
	}

	// 解析流式响应
	var conversationID string
	var messageID string

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			jsonData := strings.TrimPrefix(line, "data: ")
			var difyResp DifyStreamResponse
			if err = json.Unmarshal([]byte(jsonData), &difyResp); err != nil {
				continue // 跳过无效的JSON
			}

			switch difyResp.Event {
			case "message":
				// 流式返回响应块到客户端
				if difyResp.Answer != "" {
					if conversationID == "" {
						conversationID = difyResp.ConversationID
					}
					if messageID == "" {
						messageID = difyResp.MessageID
					}

					// Send the chunk to client
					err = stream.Send(ctx, &aichat.SendMessageResp{
						BaseResp: &base.BaseResp{
							Code: 200,
						},
						EventType: aichat.EventTypeMessage,
						Data:      difyResp.Answer,
					})
					if err != nil {
						return fmt.Errorf("failed to send stream response: %w", err)
					}
				}
			case "message_end":
				// Streaming ended, send final response if needed
				return nil
			case "error":
				// 发送错误事件到客户端
				err = stream.Send(ctx, &aichat.SendMessageResp{
					BaseResp: &base.BaseResp{
						Code:    constant.SystemError,
						Message: difyResp.Message,
					},
				})
				if err != nil {
					return fmt.Errorf("failed to send stream response: %w", err)
				}
				return fmt.Errorf("dify api错误: %s", difyResp.Message)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return nil
}

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/li1553770945/personal-aichat-service/biz/constant"
	aichat "github.com/li1553770945/personal-aichat-service/kitex_gen/aichat"
	base "github.com/li1553770945/personal-aichat-service/kitex_gen/base"
)

const difyAPIURL = "https://api.dify.ai/v1/chat-messages"

var globalHTTPClient = &http.Client{
	Timeout: 300 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	},
}

// DifyRequest Dify API 请求结构
type DifyRequest struct {
	Query          string                 `json:"query"`
	Inputs         map[string]interface{} `json:"inputs"` // 去掉 omitempty，确保传递 {}
	ResponseMode   string                 `json:"response_mode"`
	User           string                 `json:"user"`
	ConversationID string                 `json:"conversation_id,omitempty"` // 为空时不传，代表新会话
	Files          []interface{}          `json:"files,omitempty"`
}

// DifyErrorResponse Dify API 错误响应结构
type DifyErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

// DifyStreamResponse SSE 响应结构
type DifyStreamResponse struct {
	Event          string `json:"event"`
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Answer         string `json:"answer"`
	CreatedAt      int64  `json:"created_at"`
	TaskID         string `json:"task_id"`
	// Error event fields
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int    `json:"status,omitempty"`
}

// SendMessage 发送消息到 dify api 并流式返回响应
func (s *AIChatService) SendMessage(ctx context.Context, req *aichat.SendMessageReq, stream aichat.AIChatService_SendMessageServer) error {
	// 1. 参数校验
	if req.Message == "" {
		return fmt.Errorf("query message cannot be empty")
	}
	conversationId := ""
	messageId := ""
	if req.ConversationId != nil {
		conversationId = *req.ConversationId
	}
	// 2. 构建请求体
	difyReq := DifyRequest{
		Query:          req.Message,
		ConversationID: conversationId,
		Inputs:         map[string]interface{}{}, // 显式初始化为空 map
		ResponseMode:   "streaming",
		User:           "default_user_001", // 建议使用更有意义的 ID，如 req.UserId
	}

	reqBody, err := json.Marshal(difyReq)
	if err != nil {
		return fmt.Errorf("序列化请求体失败: %w", err)
	}

	// 3. 创建 HTTP 请求
	httpReq, err := http.NewRequestWithContext(ctx, "POST", difyAPIURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.Config.AIChatConfig.APIKey)

	// 4. 发送请求
	resp, err := globalHTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("HTTP 请求失败: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// 5. 错误处理
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		var errResp DifyErrorResponse
		// 尝试解析错误 JSON
		if jsonErr := json.Unmarshal(bodyBytes, &errResp); jsonErr == nil && errResp.Message != "" {
			klog.Errorf("Dify API Error: Code=%s, Msg=%s", errResp.Code, errResp.Message)
			return fmt.Errorf("dify api error: %s", errResp.Message)
		}
		// 解析失败则返回原始内容
		return fmt.Errorf("dify api status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// 6. 流式解析响应
	scanner := bufio.NewScanner(resp.Body)
	// 增加 Buffer 大小防止单行过长导致 panic
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		// SSE 格式通常以 "data: " 开头
		if !strings.HasPrefix(line, "data:") {
			continue
		}

		// 去除前缀和首尾空格
		dataStr := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if dataStr == "" {
			continue
		}

		var difyResp DifyStreamResponse
		if err := json.Unmarshal([]byte(dataStr), &difyResp); err != nil {
			klog.Warnf("unmarshal stream data failed: %v, data: %s", err, dataStr)
			continue
		}

		// 处理不同事件类型
		switch difyResp.Event {
		case "message":

			// 记录 ID 用于后续可能的逻辑
			if conversationId == "" {
				err := stream.Send(ctx, &aichat.SendMessageResp{
					BaseResp:  &base.BaseResp{Code: 200},
					EventType: constant.EventTypeConversationId,
					Data:      difyResp.ConversationID,
				})
				if err != nil {
					return fmt.Errorf("stream send agent message failed: %w", err)
				}
				conversationId = difyResp.ConversationID
			}
			if messageId == "" {
				err := stream.Send(ctx, &aichat.SendMessageResp{
					BaseResp:  &base.BaseResp{Code: 200},
					EventType: constant.EventTypeMessageId,
					Data:      difyResp.MessageID,
				})
				if err != nil {
					return fmt.Errorf("stream send agent message failed: %w", err)
				}
				messageId = difyResp.MessageID
			}

			if difyResp.Answer == "" {
				continue
			}

			// 发送流式数据给客户端
			if err := stream.Send(ctx, &aichat.SendMessageResp{
				BaseResp:  &base.BaseResp{Code: 200},
				EventType: constant.EventTypeMessage,
				Data:      difyResp.Answer,
			}); err != nil {
				return fmt.Errorf("stream send failed: %w", err)
			}

		case "message_end":
			// 消息结束，通常包含 usage 信息，这里可以选择记录日志或不做处理
			return nil

		case "agent_message": // 如果你的应用是 Agent 类型
			if err := stream.Send(ctx, &aichat.SendMessageResp{
				BaseResp:  &base.BaseResp{Code: 200},
				EventType: constant.EventTypeMessage,
				Data:      difyResp.Answer,
			}); err != nil {
				return fmt.Errorf("stream send agent message failed: %w", err)
			}

		case "error":
			klog.Errorf("dify stream error event: %s", difyResp.Message)
			if err := stream.Send(ctx, &aichat.SendMessageResp{
				BaseResp:  &base.BaseResp{Code: 200},
				EventType: constant.EventTypeError,
				Data:      difyResp.Message,
			}); err != nil {
				return fmt.Errorf("stream send agent message failed: %w", err)
			}
			return fmt.Errorf("dify stream error: %s", difyResp.Message)

		case "ping":
			// 保持连接，忽略即可
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}

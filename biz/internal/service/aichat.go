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

// DifySSEEnvelope covers both chat assistant and chatflow(workflow) streaming payloads.
// For chatflow, the important events are:
// - workflow_started / workflow_finished
// - node_started / node_finished
// - message / message_end
type DifySSEEnvelope struct {
	Event          string `json:"event"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	CreatedAt      int64  `json:"created_at"`
	TaskID         string `json:"task_id"`
	WorkflowRunID  string `json:"workflow_run_id"`

	// message event fields
	ID                   string   `json:"id"`
	Answer               string   `json:"answer"`
	FromVariableSelector []string `json:"from_variable_selector"`

	// workflow/node event payload
	Data json.RawMessage `json:"data"`

	// message_end metadata
	Metadata json.RawMessage `json:"metadata"`

	// error event fields
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int    `json:"status,omitempty"`
}

type difyWorkflowStartedData struct {
	ID         string                 `json:"id"`
	WorkflowID string                 `json:"workflow_id"`
	Inputs     map[string]interface{} `json:"inputs"`
	CreatedAt  int64                  `json:"created_at"`
}

type difyWorkflowFinishedData struct {
	ID         string                 `json:"id"`
	WorkflowID string                 `json:"workflow_id"`
	Status     string                 `json:"status"`
	Outputs    map[string]interface{} `json:"outputs"`
	Error      interface{}            `json:"error"`
	Elapsed    float64                `json:"elapsed_time"`
	TotalSteps int                    `json:"total_steps"`
}

type difyNodeData struct {
	ID       string `json:"id"`
	NodeID   string `json:"node_id"`
	NodeType string `json:"node_type"`
	Title    string `json:"title"`
	Index    int    `json:"index"`

	Status      string      `json:"status,omitempty"`
	Error       interface{} `json:"error,omitempty"`
	ElapsedTime float64     `json:"elapsed_time,omitempty"`
}

type outgoingWorkflowEvent struct {
	Event         string  `json:"event"`
	WorkflowRunID string  `json:"workflow_run_id,omitempty"`
	WorkflowID    string  `json:"workflow_id,omitempty"`
	TaskID        string  `json:"task_id,omitempty"`
	CreatedAt     int64   `json:"created_at,omitempty"`
	Status        string  `json:"status,omitempty"`
	ElapsedTime   float64 `json:"elapsed_time,omitempty"`
	TotalSteps    int     `json:"total_steps,omitempty"`
}

type outgoingNodeEvent struct {
	Event         string  `json:"event"`
	WorkflowRunID string  `json:"workflow_run_id,omitempty"`
	TaskID        string  `json:"task_id,omitempty"`
	NodeID        string  `json:"node_id,omitempty"`
	NodeType      string  `json:"node_type,omitempty"`
	Title         string  `json:"title,omitempty"`
	Index         int     `json:"index,omitempty"`
	Status        string  `json:"status,omitempty"`
	ElapsedTime   float64 `json:"elapsed_time,omitempty"`
}

func sendStreamEvent(ctx context.Context, stream aichat.AIChatService_SendMessageServer, eventType string, data string) error {
	return stream.Send(ctx, &aichat.SendMessageResp{
		BaseResp:  &base.BaseResp{Code: 200},
		EventType: eventType,
		Data:      data,
	})
}

func shouldForwardChatflowMessage(envelope DifySSEEnvelope) bool {
	// For chatflow, "message" chunks may come from different nodes/variables.
	// We only treat the LLM node streaming output (usually variable "text") as user-visible chat content.
	if len(envelope.FromVariableSelector) >= 2 {
		return envelope.FromVariableSelector[1] == "text"
	}
	// Backward compatibility: assistant apps may not include from_variable_selector.
	return true
}

// SendMessage 发送消息到 dify api 并流式返回响应
func (s *AIChatService) SendMessage(ctx context.Context, req *aichat.SendMessageReq, stream aichat.AIChatService_SendMessageServer) error {
	// 1. 参数校验
	if req.Message == "" {
		return fmt.Errorf("query message cannot be empty")
	}
	conversationId := ""
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
	// 增加 Buffer 大小防止单行过长导致 scanner 停止（chatflow 的 node_finished/outputs 可能很大）
	buf := make([]byte, 0, 256*1024)
	scanner.Buffer(buf, 10*1024*1024)

	sentConversationID := conversationId != ""
	sentMessageID := false

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

		// Some SSE servers may send keep-alive payloads like [DONE]
		if dataStr == "[DONE]" {
			continue
		}

		var envelope DifySSEEnvelope
		if err := json.Unmarshal([]byte(dataStr), &envelope); err != nil {
			klog.Warnf("unmarshal stream data failed: %v, data: %s", err, dataStr)
			continue
		}

		// best-effort: send conversationId/messageId as early as possible
		if !sentConversationID && envelope.ConversationID != "" {
			if err := sendStreamEvent(ctx, stream, constant.EventTypeConversationId, envelope.ConversationID); err != nil {
				return fmt.Errorf("stream send conversationId failed: %w", err)
			}
			sentConversationID = true
			conversationId = envelope.ConversationID
		}
		if !sentMessageID && envelope.MessageID != "" {
			if err := sendStreamEvent(ctx, stream, constant.EventTypeMessageId, envelope.MessageID); err != nil {
				return fmt.Errorf("stream send messageId failed: %w", err)
			}
			sentMessageID = true
		}

		// 处理不同事件类型
		switch envelope.Event {
		case "message", "agent_message":
			if envelope.Answer == "" {
				continue
			}
			if !shouldForwardChatflowMessage(envelope) {
				continue
			}
			if err := sendStreamEvent(ctx, stream, constant.EventTypeMessage, envelope.Answer); err != nil {
				return fmt.Errorf("stream send message failed: %w", err)
			}

		case "workflow_started":
			var wf difyWorkflowStartedData
			if len(envelope.Data) > 0 {
				_ = json.Unmarshal(envelope.Data, &wf)
			}
			payload := outgoingWorkflowEvent{
				Event:         envelope.Event,
				WorkflowRunID: envelope.WorkflowRunID,
				WorkflowID:    wf.WorkflowID,
				TaskID:        envelope.TaskID,
				CreatedAt:     envelope.CreatedAt,
			}
			b, _ := json.Marshal(payload)
			if err := sendStreamEvent(ctx, stream, constant.EventTypeWorkflowStarted, string(b)); err != nil {
				return fmt.Errorf("stream send workflowStarted failed: %w", err)
			}

		case "workflow_finished":
			var wf difyWorkflowFinishedData
			if len(envelope.Data) > 0 {
				_ = json.Unmarshal(envelope.Data, &wf)
			}
			payload := outgoingWorkflowEvent{
				Event:         envelope.Event,
				WorkflowRunID: envelope.WorkflowRunID,
				WorkflowID:    wf.WorkflowID,
				TaskID:        envelope.TaskID,
				CreatedAt:     envelope.CreatedAt,
				Status:        wf.Status,
				ElapsedTime:   wf.Elapsed,
				TotalSteps:    wf.TotalSteps,
			}
			b, _ := json.Marshal(payload)
			if err := sendStreamEvent(ctx, stream, constant.EventTypeWorkflowFinished, string(b)); err != nil {
				return fmt.Errorf("stream send workflowFinished failed: %w", err)
			}

		case "node_started":
			var node difyNodeData
			if len(envelope.Data) > 0 {
				_ = json.Unmarshal(envelope.Data, &node)
			}
			payload := outgoingNodeEvent{
				Event:         envelope.Event,
				WorkflowRunID: envelope.WorkflowRunID,
				TaskID:        envelope.TaskID,
				NodeID:        node.NodeID,
				NodeType:      node.NodeType,
				Title:         node.Title,
				Index:         node.Index,
			}
			b, _ := json.Marshal(payload)
			if err := sendStreamEvent(ctx, stream, constant.EventTypeNodeStarted, string(b)); err != nil {
				return fmt.Errorf("stream send nodeStarted failed: %w", err)
			}

		case "node_finished":
			var node difyNodeData
			if len(envelope.Data) > 0 {
				_ = json.Unmarshal(envelope.Data, &node)
			}
			payload := outgoingNodeEvent{
				Event:         envelope.Event,
				WorkflowRunID: envelope.WorkflowRunID,
				TaskID:        envelope.TaskID,
				NodeID:        node.NodeID,
				NodeType:      node.NodeType,
				Title:         node.Title,
				Index:         node.Index,
				Status:        node.Status,
				ElapsedTime:   node.ElapsedTime,
			}
			b, _ := json.Marshal(payload)
			if err := sendStreamEvent(ctx, stream, constant.EventTypeNodeFinished, string(b)); err != nil {
				return fmt.Errorf("stream send nodeFinished failed: %w", err)
			}

		case "message_end":
			// metadata can be very large; forward as-is for clients who need usage/token info
			meta := "{}"
			if len(envelope.Metadata) > 0 {
				meta = string(envelope.Metadata)
			}
			if err := sendStreamEvent(ctx, stream, constant.EventTypeMessageEnd, meta); err != nil {
				return fmt.Errorf("stream send messageEnd failed: %w", err)
			}

		case "error":
			msg := envelope.Message
			if msg == "" {
				msg = "dify stream error"
			}
			klog.Errorf("dify stream error event: code=%s status=%d message=%s", envelope.Code, envelope.Status, msg)
			_ = sendStreamEvent(ctx, stream, constant.EventTypeError, msg)
			return fmt.Errorf("dify stream error: %s", msg)

		case "ping":
			continue
		default:
			// ignore unknown events for forward compatibility
			continue
		}

	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	return nil
}

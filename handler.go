package main

import (
	"context"
	"fmt"

	"github.com/li1553770945/personal-aichat-service/biz/infra/container"
	aichat "github.com/li1553770945/personal-aichat-service/kitex_gen/aichat"
)

// AIChatServiceImpl implements the last service interface defined in the IDL.
type AIChatServiceImpl struct{}

func (s *AIChatServiceImpl) SendMessage(ctx context.Context, req *aichat.SendMessageReq, stream aichat.AIChatService_SendMessageServer) (err error) {
	App := container.GetGlobalContainer()

	// Call service layer to send message to Dify API and stream response
	err = App.AIChatService.SendMessage(ctx, req, stream)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

package main

import (
	"context"

	"github.com/li1553770945/personal-aichat-service/biz/infra/container"
	aichat "github.com/li1553770945/personal-aichat-service/kitex_gen/aichat"
)

// AIChatServiceImpl implements the last service interface defined in the IDL.
type AIChatServiceImpl struct{}

// SendMessage implements the AIChatServiceImpl interface.
func (s *AIChatServiceImpl) SendMessage(ctx context.Context, req *aichat.SendMessageReq) (resp *aichat.SendMessageResp, err error) {
	App := container.GetGlobalContainer()
	resp, err = App.AIChatService.SendMessage(ctx, req)
	return
}

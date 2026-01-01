package service

import (
	"context"

	"github.com/li1553770945/personal-aichat-service/kitex_gen/aichat"
	"github.com/li1553770945/sheepim-auth-service/kitex_gen/auth/authservice"
)

type AIChatService struct {
	AuthClient authservice.Client
}

type IAIChatService interface {
	SendMessage(ctx context.Context, req *aichat.SendMessageReq) (resp *aichat.SendMessageResp, err error)
}

func NewAChatService(authClient authservice.Client) IAIChatService {
	return &AIChatService{
		AuthClient: authClient,
	}
}

package service

import (
	"context"

	"github.com/li1553770945/personal-aichat-service/biz/infra/config"
	"github.com/li1553770945/personal-aichat-service/kitex_gen/aichat"
	"github.com/li1553770945/sheepim-auth-service/kitex_gen/auth/authservice"
)

type AIChatService struct {
	AuthClient authservice.Client
	Config     *config.Config
}

type IAIChatService interface {
	SendMessage(ctx context.Context, req *aichat.SendMessageReq, stream aichat.AIChatService_SendMessageServer) error
}

func NewAChatService(authClient authservice.Client, config *config.Config) IAIChatService {
	return &AIChatService{
		AuthClient: authClient,
		Config:     config,
	}
}

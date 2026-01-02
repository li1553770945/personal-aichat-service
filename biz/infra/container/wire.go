//go:build wireinject
// +build wireinject

package container

import (
	"github.com/google/wire"
	"github.com/li1553770945/personal-aichat-service/biz/infra/config"
	"github.com/li1553770945/personal-aichat-service/biz/infra/log"
	"github.com/li1553770945/personal-aichat-service/biz/infra/rpc"
	"github.com/li1553770945/personal-aichat-service/biz/infra/trace"
	"github.com/li1553770945/personal-aichat-service/biz/internal/service"
)

func GetContainer(env string) *Container {
	panic(wire.Build(

		//infra
		config.GetConfig,
		log.InitLog,
		trace.InitTrace,
		rpc.NewAuthClient,

		//service
		service.NewAChatService,

		NewContainer,
	))
}

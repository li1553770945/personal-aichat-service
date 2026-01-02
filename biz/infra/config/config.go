package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/li1553770945/personal-aichat-service/biz/constant"
	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	ServiceName   string `yaml:"service-name"`
	ListenAddress string `yaml:"listen-address"`
}

type OpenTelemetryConfig struct {
	Endpoint string `yaml:"endpoint"`
}

type EtcdConfig struct {
	Endpoint []string `yaml:"endpoint"`
}

type RpcConfig struct {
	AuthServiceName string `yaml:"auth-service-name"`
}

type AIChatConfig struct {
	Model  string `yaml:"model"`
	APIKey string `yaml:"api-key"`
}

type Config struct {
	Env                 string
	ServerConfig        ServerConfig        `yaml:"server"`
	OpenTelemetryConfig OpenTelemetryConfig `yaml:"open-telemetry"`
	RpcConfig           RpcConfig           `yaml:"rpc"`
	EtcdConfig          EtcdConfig          `yaml:"etcd"`
	AIChatConfig        AIChatConfig        `yaml:"aichat"`
}

func GetConfig(env string) *Config {
	if env != constant.EnvProduction && env != constant.EnvDevelopment {
		panic(fmt.Sprintf("环境必须是%s或者%s之一", constant.EnvProduction, constant.EnvDevelopment))
	}
	conf := &Config{}
	path := filepath.Join("conf", fmt.Sprintf("%s.yml", env))
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	err = yaml.NewDecoder(f).Decode(conf)
	conf.Env = env
	if err != nil {
		panic(err)
	}

	return conf
}

# 个人网站-AI聊天服务

## 初始化项目
```bash
kitex -streamx -module "github.com/li1553770945/personal-aichat-service" -service personal-aichat-service idl/aichat.thrift
cd biz/infra/container
wire
```
## 配置文件示例

```yml
server:
  listen-address: 0.0.0.0:8894
  service-name: personal-aichat-service

etcd:
  endpoint:
    - 127.0.0.1:2379

```

## 开发环境

```bash
export ENV=development
```

## 生产环境

```bash
export ENV=production
```
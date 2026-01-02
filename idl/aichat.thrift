namespace go aichat
include "base.thrift"


struct SendMessageReq{
    1: required string message
}

struct SendMessageResp{
    1: required base.BaseResp baseResp
}


service AIChatService {
    SendMessageResp SendMessage(SendMessageReq req)(streaming.mode="server")
}

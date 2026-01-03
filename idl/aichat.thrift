namespace go aichat
include "base.thrift"


struct SendMessageReq{
    1: required string message
    2: optional string conversation_id
}

struct SendMessageResp{
    1: required base.BaseResp baseResp
    2: required string event_type
    3: required string data
}


service AIChatService {
    SendMessageResp SendMessage(SendMessageReq req)(streaming.mode="server")
}

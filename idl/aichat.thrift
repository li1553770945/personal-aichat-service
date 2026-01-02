namespace go aichat
include "base.thrift"


struct SendMessageReq{
    1: required string message
}
const string EventTypeMessage = "message"
const string EventTypeEventId = "event_id"
const string EventTypeMCP = "mcp"

struct SendMessageResp{
    1: required base.BaseResp baseResp
    2: required string event_type
    3: required string data
}


service AIChatService {
    SendMessageResp SendMessage(SendMessageReq req)(streaming.mode="server")
}

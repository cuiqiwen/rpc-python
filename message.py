"""
基于二进制传输的RPC框架实现
消息协议
序列化方式采用json（还有其他很多方式）
by cuiqiwenfirst@qq.com
"""
import json
import struct


class RpcMessage:
    """消息模块"""
    RESPONSE_HEAD_LENGTH = 4
    # 通信协议 请求-->响应
    COMMAND_PING = "ping"
    COMMAND_HELLO = "hello"
    CommandMap = {
        COMMAND_PING: "pong",
        COMMAND_HELLO: "world"
    }

    def __init__(self):
        self.__desc = "消息协议"

    @classmethod
    def request(cls, in_put: str, params: str) -> tuple[bytes, bytes]:
        """构造请求"""
        request_bytes = json.dumps({"in": in_put, "params": params}).encode("utf-8")
        length_prefix = struct.pack("I", len(request_bytes))  # 请求长度前缀

        return length_prefix, request_bytes

    @classmethod
    def input(cls, data: bytes):
        """请求信息"""
        input_dict = json.loads(data)

        return input_dict.get("in"), input_dict.get("params")

    @classmethod
    def output(cls, data: bytes) -> tuple[str, str]:
        """响应信息"""
        output_dict = json.loads(data) if data else {}

        return output_dict.get("out"), output_dict.get("result")

    @classmethod
    def response(cls, out_put, result) -> tuple[bytes, bytes]:
        """rpc响应"""
        response_bytes = json.dumps({"out": out_put, "result": result}).encode("utf-8")
        length_prefix = struct.pack("I", len(response_bytes))  # 响应长度前缀

        return length_prefix, response_bytes

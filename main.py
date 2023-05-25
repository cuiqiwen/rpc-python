"""
基于二进制传输的RPC框架实现
服务端
by cuiqiwenfirst@qq.com
"""
from server import RpcServer
from message import RpcMessage


def ping(conn, params):
    """处理ping指令函数"""
    length, response = RpcMessage.response("pong", params)
    conn.sendall(length)
    conn.sendall(response)


def test(params):
    """执行请求业务处理"""

    return f"{params} +++ world"


def hello(conn, params):
    """处理hello指令函数"""
    length, response = RpcMessage.response("world", params)
    conn.sendall(length)
    conn.sendall(response)


if __name__ == "__main__":
    """启动入口"""
    # 启动RPC服务
    print("starting rpc server")
    server = RpcServer()
    # 注册handler函数
    server.register_handler(RpcMessage.COMMAND_PING, ping)
    server.register_handler(RpcMessage.COMMAND_HELLO, hello)
    server.run(using_pre_forking=True, workers=3)

"""
基于二进制传输的RPC框架实现
客户端
by cuiqiwenfirst@qq.com
"""
import socket
import struct
import time
from message import RpcMessage

"""
def receive(sock, n):
    rs = []  # 读取的结果
    while n > 0:
        r = sock.recv(n)
        if not r:  # EOF
            return rs
        rs.append(r)
        n -= len(r)
    return ''.join(rs)
"""


class RpcClient:
    """客户端"""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def __connect(self, host: str, port: int):
        """
        连接
        host: localhost
        port: 8080
        """
        return self.socket.connect((host, port))

    def __close(self):
        """关闭套接字"""
        self.socket.close()

    def __enter__(self):
        """创建套接字"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__connect(self.host, self.port)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__close()

    def rpc(self, in_, params):
        """RPC 远程调用"""
        # 请求长度前缀 请求消息体
        length_prefix, request_body = RpcMessage.request(in_, params)
        self.socket.sendall(length_prefix)
        self.socket.sendall(request_body)
        # 响应长度前缀
        length_prefix = self.socket.recv(RpcMessage.RESPONSE_HEAD_LENGTH)
        length, = struct.unpack("I", length_prefix)
        # 响应消息体
        out_, result = RpcMessage.output(self.socket.recv(length))
        # 返回响应类型和结果
        return out_, result


if __name__ == "__main__":

    def test_case():
        """模拟用例"""
        with RpcClient("localhost", 8080) as client:
            print("客户端连接成功...")
            for i in range(10):  # 连续发送 10 个 rpc 请求
                out, result = client.rpc(RpcMessage.COMMAND_PING, "ireader %d" % i)
                print(out, result)
                # out, result = client.rpc("test", "ireader %d" % i)
                # print(out, result)
                # 休眠 1s，便于观察
                time.sleep(1)

    # 测试
    test_case()

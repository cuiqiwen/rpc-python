"""
基于二进制传输的RPC框架实现
服务端
by cuiqiwenfirst@qq.com
"""
import os
from typing import Callable
import socket
import struct
import threading
import multiprocessing
from message import RpcMessage


class RpcServer:
    """服务"""
    socket = None
    # 命令执行函数
    handlers = {}

    def register_handler(self, command: str, handler: Callable):
        """注册处理函数"""
        self.handlers[command] = handler

    def dispatch_handler(self, conn, client_addr):
        """分发主线程的接收到的连接"""
        # 同步单线程处理 单个主线程效率低下 下一个连接到来 只能等待上一个连接处理完成 才能被处理 不适合生产环境
        # self.execute_handler(conn, client_addr)

        # 同步多线程处理
        th = threading.Thread(target=self.execute_handler, args=(conn, client_addr))
        th.start()

    def execute_handler(self, conn, client_addr):
        """处理单个连接"""
        print(f"正在处理客户端{client_addr}的连接")
        while True:
            length_prefix = conn.recv(RpcMessage.RESPONSE_HEAD_LENGTH)
            if not length_prefix:
                print(f"{client_addr} 连接断开")
                conn.close()
                break  # 关闭连接，继续处理下一个连接
            length, = struct.unpack("I", length_prefix)
            body = conn.recv(length)
            _in, params = RpcMessage.input(body)
            handler = self.handlers.get(_in)
            if handler:
                handler(conn, params)
            else:
                print(f"没有可处理的函数！请提前register_handler")
                print(f"{client_addr} 连接断开")
                conn.close()
                break

    def run(self, host: str = "localhost", port: int = 8080, using_pre_forking: bool = False):
        """
        run server
        绑定服务端端口
        开启连接监听
        """
        # TCP套接字
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((host, port))
        print(f"starting on {host}:{port}")
        self.socket.listen(1)
        print(f"listening..")
        # 开启进程池 实现 PreForking模型下 多进程多线程处理
        if using_pre_forking:
            print(f"父进程pid:{os.getpid()}")
            process_pool = multiprocessing.Pool(2)
            for i in range(2):
                process_pool.apply_async(self.process_loop_accept, args=(i, self))
            process_pool.close()
            process_pool.join()
        else:
            self.loop_accept()

    def loop_accept(self):
        """循环抢夺连接资源"""
        while True:
            print(f"等待下一个连接中...")
            conn, client_addr = self.socket.accept()
            self.dispatch_handler(conn, client_addr)

    @classmethod
    def process_loop_accept(cls, i: int, self):
        """多进程抢夺连接资源"""
        print(f"子进程编号{i}启动 pid: {os.getpid()}")
        new_server = self
        while True:
            conn, client_addr = new_server.socket.accept()
            new_server.dispatch_handler(conn, client_addr)

"""
基于二进制传输的RPC框架实现
服务端
by cuiqiwenfirst@qq.com
"""
import os
import sys
from typing import Callable
import signal
import socket
import struct
import threading
import multiprocessing
from message import RpcMessage


class RpcPingHandler:
    """请求处理 ping指令handler类"""
    actions = {}

    def __init__(self, client_socket: socket, request_params: str):
        self.client_socket = client_socket
        self.request_params = request_params

    def add_action(self, action: Callable):
        """添加自定义处理"""
        self.actions[f"{action}"] = action

    def before_request(self):
        """处理请求之前"""
        print("before_request")

    def after_request(self):
        """处理请求之后"""
        print("after_request")

    def do_action(self, action: str):
        """处理请求"""
        pass


class RpcServer:
    """服务"""
    socket = None
    request_queue_size = 1
    workers = multiprocessing.cpu_count() * 2  # 子进程个数
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

    def run(self, host: str = "localhost", port: int = 8080, using_pre_forking: bool = False, workers: int = 0):
        """
        run server
        绑定服务端端口
        开启连接监听
        """
        # TCP套接字
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((host, port))
        if workers > 0:
            self.workers = workers
        print(f"starting on {host}:{port}")
        self.socket.listen(self.request_queue_size)
        print(f"listening..")
        # 开启进程池 实现 PreForking模型下 多进程多线程处理
        if using_pre_forking:
            self.multiprocess_loop_accept()
        else:
            self.loop_accept()

    def loop_accept(self):
        """循环抢夺连接资源"""
        while True:
            print(f"进程id: {os.getpid()}等待下一个连接中...")
            conn, client_addr = self.socket.accept()
            self.dispatch_handler(conn, client_addr)

    def multiprocess_loop_accept(self):
        """多进程抢夺连接资源"""
        flag = True

        def child_exit_handler():
            """子进程退出信号处理"""
            print("子进程退出信号处理")

        # 创建指定数量的子进程用于处理客户端连接
        for _ in range(self.workers):
            pid = os.fork()  # 此时创建当前进程的副本
            if pid == 0:  # pid==0 则为子进程
                self.loop_accept()
                sys.exit(0)
        # 父进程执行流
        print(f"父进程pid:{os.getpid()}")
        # 父进程等待子进程退出
        signal.signal(signal.SIGCHLD, child_exit_handler)
        while flag:
            try:
                pid, status = os.wait()
                print("子进程收割完成，即将退出")
            except OSError:
                break
        print("服务退出")

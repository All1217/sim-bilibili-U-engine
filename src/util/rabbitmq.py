# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : rabbitmq.py
# @Project : algorithm-engine

import pika
import json
import traceback
import threading
from src.util.profileBuilder import buildOne
from concurrent.futures import ThreadPoolExecutor
import src.config.application as config


class RabbitMQListener:
    # RabbitMQ监听器
    def __init__(self, prefetch_count, max_workers):
        self.config = {
            'username': config.RABBIT_USERNAME,
            'password': config.RABBIT_PASS,
            'host': config.RABBIT_HOST,
            'port': config.RABBIT_PORT,
            'virtual-host': config.RABBIT_VIRTUAL_HOST
        }
        self.prefetch_count = prefetch_count
        self.connection = None
        self.channel = None
        self.should_stop = False
        self.thread = None
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = 0
        self.task_lock = threading.Lock()

    def connect(self):
        """建立RabbitMQ连接"""
        try:
            credentials = pika.PlainCredentials(self.config['username'], self.config['password'])
            parameters = pika.ConnectionParameters(
                host=self.config['host'],
                port=self.config['port'],
                virtual_host=self.config['virtual-host'],
                credentials=credentials,
                heartbeat=60,
                connection_attempts=3,
                retry_delay=5,
                socket_timeout=10,
                blocked_connection_timeout=300,
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            print(f"✅ RabbitMQ连接成功")
            return True
        except Exception as e:
            print(f"❌ RabbitMQ连接失败: {e}")
            return False

    def processerAsync(self, ch, method, properties, body):
        """
        异步处理任务（在线程池中执行）
        """

        def task():
            try:
                # 解析消息
                if isinstance(body, bytes):
                    message = json.loads(body.decode('utf-8'))
                else:
                    message = json.loads(body)
                uid = message.get('uid')
                # 执行画像构建
                buildOne(uid, save_to_db=True)
                # 确认消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"❌ 处理用户 {uid if 'uid' in locals() else 'unknown'} 失败: {e}")
                traceback.print_exc()
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            finally:
                with self.task_lock:
                    self.active_tasks -= 1
        # 更新活跃任务计数
        with self.task_lock:
            self.active_tasks += 1
        # 提交到线程池
        self.executor.submit(task)

    def startListen(self):
        """开始监听队列"""
        if not self.connect():
            print("❌ 无法连接RabbitMQ")
            return
        try:
            # 声明队列和交换机
            exchange_name = 'userProfile.exchange'
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
            self.channel.queue_declare(queue='userProfile.queue1', durable=True)
            self.channel.queue_bind(exchange=exchange_name, queue='userProfile.queue1', routing_key='userProfile')
            # 设置消费者
            self.channel.basic_consume(queue='userProfile.queue1', on_message_callback=self.processerAsync,
                                       auto_ack=False)
            print(f"RabbitMQ监听器已启动")
            self.channel.start_consuming()
        except Exception as e:
            print(f"❌ 监听过程出错: {e}")
        finally:
            self.stop()

    def start(self):
        """在后台线程中启动监听器"""
        self.thread = threading.Thread(target=self.startListen, daemon=True)
        self.thread.start()
        print("🔄 RabbitMQ监听器已在后台线程启动")

    def stop(self):
        """停止监听器"""
        self.should_stop = True
        # 关闭线程池
        self.executor.shutdown(wait=True, cancel_futures=False)
        print("🔄 线程池已关闭")

        # 关闭RabbitMQ连接
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        print("🛑 RabbitMQ监听器已停止")


# 全局单例
_rabbitmq_listener = None


def getRabbitmqListener(prefetch=10, maxWorkers=5):
    """获取RabbitMQ监听器单例"""
    global _rabbitmq_listener
    if _rabbitmq_listener is None:
        try:
            _rabbitmq_listener = RabbitMQListener(config.RABBIT_PREFETCH, config.RABBIT_MAX_WORKERS)
        except Exception as e:
            print(e)
            print('从配置读取prefetch和maxWorkers失败，尝试传入默认值')
            _rabbitmq_listener = RabbitMQListener(prefetch, maxWorkers)
    return _rabbitmq_listener


def startRabbitmq():
    """启动RabbitMQ监听器（后台线程）"""
    listener = getRabbitmqListener()
    listener.start()
    return listener


def stopRabbitmq():
    listener = getRabbitmqListener()
    listener.stop()

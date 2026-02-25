# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : database.py
# @Project : algorithm-engine

import pymysql
from contextlib import contextmanager
from pymysql.constants import CLIENT
from dbutils.pooled_db import PooledDB
from elasticsearch import Elasticsearch
import redis
import src.config.application as config
import threading
import atexit


def get_mysql_conn():
    return get_mysql_pool().get_connection()


def get_redis_client():
    return get_redis_pool().get_connection()


# 保持向后兼容
def connectMySql():
    return get_mysql_conn()


def connectRedis():
    return get_redis_client()


class MySQLPool:
    """MySQL连接池单例（使用线程安全的单例模式）"""
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self._init_pool()
                    self._initialized = True

    def _init_pool(self):
        print("🚀 初始化MySQL连接池...")
        self._pool = PooledDB(
            creator=pymysql,
            maxconnections=20,
            mincached=5,
            maxcached=10,
            maxshared=0,
            blocking=True,
            maxusage=None,
            setsession=[],
            ping=1,
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DATABASE,
            port=3306,
            charset='utf8mb4',
            autocommit=True,
            client_flag=CLIENT.MULTI_STATEMENTS,
            cursorclass=pymysql.cursors.DictCursor
        )
        print(f"✅ MySQL连接池初始化完成，最大连接数: 20")

    def get_connection(self):
        return self._pool.connection()

    def close_all(self):
        if hasattr(self, '_pool') and self._pool:
            self._pool.close()
            print("✅ MySQL连接池已关闭")


class RedisPool:
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self._init_pool()
                    self._initialized = True

    def _init_pool(self):
        print("🚀 初始化Redis连接池...")
        self._pool = redis.ConnectionPool(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True,
            max_connections=20,
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
        )
        print(f"✅ Redis连接池初始化完成，最大连接数: 20")

    def get_connection(self):
        return redis.Redis(connection_pool=self._pool)

    def close_all(self):
        if hasattr(self, '_pool') and self._pool:
            self._pool.disconnect()
            print("✅ Redis连接池已关闭")


_mysql_pool = None
_redis_pool = None


def init_pools():
    """初始化连接池（在应用启动时调用）"""
    global _mysql_pool, _redis_pool
    if _mysql_pool is None:
        _mysql_pool = MySQLPool()
    if _redis_pool is None:
        _redis_pool = RedisPool()
    print("🎯 所有连接池初始化完成")
    return _mysql_pool, _redis_pool


def get_mysql_pool():
    """获取MySQL连接池单例"""
    global _mysql_pool
    if _mysql_pool is None:
        _mysql_pool = MySQLPool()
    return _mysql_pool


def get_redis_pool():
    """获取Redis连接池单例"""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = RedisPool()
    return _redis_pool


@contextmanager
def mysql_cursor():
    """获取MySQL游标的上下文管理器"""
    conn = get_mysql_conn()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@contextmanager
def mysql_conn():
    """获取MySQL连接的上下文管理器"""
    conn = get_mysql_conn()
    try:
        yield conn
    finally:
        conn.close()


def close_all_pools():
    """关闭所有连接池（应用退出时调用）"""
    print("\n🔄 正在关闭连接池...")
    global _mysql_pool, _redis_pool
    if _mysql_pool:
        _mysql_pool.close_all()
    if _redis_pool:
        _redis_pool.close_all()
    print("✅ 所有连接池已关闭")


# 注册退出时的清理函数
atexit.register(close_all_pools)

# elasticsearch包已内置连接池，不用我们手动写
ES_LINK = 'http://' + config.ES_HOST + ':' + str(config.ES_PORT)
es_client = Elasticsearch(
    ES_LINK,
    maxsize=20,
    retry_on_timeout=True,
    timeout=30
)

def getES():
    """直接返回ES客户端（自动管理连接池）"""
    return es_client

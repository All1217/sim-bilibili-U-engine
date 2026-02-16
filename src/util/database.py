# _*_ coding : utf-8 _*_
# @Time : 2025/2/19 19:38
# @Author : Morton
# @File : database
# @Project : recommendation-algorithm

import pymysql
from pymysql.constants import CLIENT
from src.common.redisConstants import BEHAVIOR_THRESHOLD_KEY
import src.config.application as config
import redis


def connectMySql():
    DB = pymysql.connect(host=config.HOST, user=config.USER,
                         password=config.PASSWORD,
                         database=config.DATABASE,
                         autocommit=True,
                         client_flag=CLIENT.MULTI_STATEMENTS)
    return DB

# 自定义参数连接
def customizeMySqlConn(host, user, password, database, autocommit: bool):
    DB = pymysql.connect(host=host, user=user,
                         password=password,
                         database=database,
                         autocommit=autocommit,
                         client_flag=CLIENT.MULTI_STATEMENTS)
    return DB

def connectRedis(autoDecode = True):
    r = redis.Redis(host=config.REDIS_HOST,
                    port=config.REDIS_PORT,
                    db=config.REDIS_DB,
                    password=config.REDIS_PASSWORD,
                    decode_responses=autoDecode # 自动解码
                    )
    return r

if __name__ == '__main__':
    r = connectRedis()
    temp = r.hgetall(BEHAVIOR_THRESHOLD_KEY)
    print(temp)
    THRESHOLDS = {
        'active_threshold': float(temp.get('active_threshold', 0)),
        'passive_threshold': float(temp.get('passive_threshold', 0)),
        'night_ratio_threshold': float(temp.get('night_ratio_threshold', 0)),
        'night_min_samples': int(temp.get('night_min_samples', 0))
    }
    print(THRESHOLDS)

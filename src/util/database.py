# _*_ coding : utf-8 _*_
# @Time : 2025/2/19 19:38
# @Author : Morton
# @File : database
# @Project : recommendation-algorithm

import pymysql
from pymysql.constants import CLIENT
import src.config.application as config


def connect():
    DB = pymysql.connect(host=config.HOST, user=config.USER,
                         password=config.PASSWORD,
                         database=config.DATABASE,
                         autocommit=True,
                         client_flag=CLIENT.MULTI_STATEMENTS)
    return DB

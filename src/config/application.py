# _*_ coding : utf-8 _*_
# @Time : 2025/2/19 19:33
# @Author : Morton
# @File : application
# @Project : algorithm-engine

import os
from dotenv import load_dotenv

load_dotenv()

"""
    数据库链接参数
"""
# mysql
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = '123456'
MYSQL_DATABASE = 'video'
MYSQL_PORT = 3306
# redis
REDIS_PORT = 6379
REDIS_HOST = '192.168.150.102'
REDIS_DB = 4
REDIS_PASSWORD = '123456'
# elasticsearch
ES_HOST = '192.168.150.102'
ES_PORT = 9200
"""
    数据项权重
"""
LIKE_WEIGHT = 1.5
COIN_WEIGHT = 2
PLAY_WEIGHT = 1
COLLECT_WEIGHT = 1

"""
    爬虫配置
"""
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
COOKIE = "buvid4=923B8568-A113-052D-1A4F-E2651C92732020713-022071222-AUsgFISWH6h0c43lCyZqCA%3D%3D; CURRENT_BLACKGAP=0; buvid_fp_plain=undefined; is-2022-channel=1; buvid3=3F2F38F1-15FA-F481-D7C8-20644156094312339infoc; b_nut=1720781412; _uuid=FC7E9DC6-C78D-4649-23C4-8E6715A4758808839infoc; header_theme_version=CLOSE; hit-dyn-v2=1; iflogin_when_web_push=0; enable_web_push=DISABLE; LIVE_BUVID=AUTO1417336487702172; rpdid=|(umRYYmmmuu0J'u~J~~J~Y|k; historyviewmode=list; DedeUserID=104212331; DedeUserID__ckMd5=8f18f859a5aa5345; enable_feed_channel=ENABLE; go-old-space=1; PVID=7; CURRENT_QUALITY=0; home_feed_column=5; browser_resolution=1528-738; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDY1OTY2MDgsImlhdCI6MTc0NjMzNzM0OCwicGx0IjotMX0.3GbZCUkWAABn75yPb2E6D-1oA9c7Ktj4Ffpp9766LKA; bili_ticket_expires=1746596548; fingerprint=06b6c1573327369e8183fcbc7ae24b85; buvid_fp=06b6c1573327369e8183fcbc7ae24b85; SESSDATA=28913de1%2C1762095890%2C8d17e%2A51CjC3_O20lq-MFaFgWYYAPZUq7ZH-Qc_lw8fDi0Rv_-r-jARcbYkible3BLw7bzDKqLQSVnNtS0Y3ZS11alFHNFlrQkhabGQwVi1MUndEei1XZllXeGVsQmdoNkFUZkdDZnFXamxZYmt5bW1NWEp5REtNWUVBMHpWTW82dlhJZDdWcEJ2OGo4S1d3IIEC; bili_jct=e5ad229b34ad7fc05ed8b02097bf1093; bp_t_offset_104212331=1064023425405681664; CURRENT_FNVAL=2000; b_lsid=43EAA2F5_196A85076BB; bmg_af_switch=1; bmg_src_def_domain=i0.hdslb.com; sid=qe4ayrne"
REFERER = "https://www.bilibili.com/"

"""
    定时间隔
"""
TASK_GAP = 60

"""
    本机服务配置
"""
FLASK_PORT = 5000
FLASK_HOST = "0.0.0.0"

"""
    RabbitMQ配置
"""
RABBIT_HOST = '192.168.150.102'
RABBIT_PORT = '5672'
RABBIT_VIRTUAL_HOST = 'AniLink'
RABBIT_USERNAME = 'admin'
RABBIT_PASS = 'admin123'
RABBIT_PREFETCH = 10
RABBIT_MAX_WORKERS = 5
"""
    大模型配置
"""
DEFAULT_MODEL = "text-embedding-v4"
MODEL_HOST = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_DIMENSION = 1024
# API key需要自己在环境变量设置：DASHSCOPE_API_KEY=${your_key}
DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY')

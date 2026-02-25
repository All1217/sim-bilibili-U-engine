# _*_ coding : utf-8 _*_
# @Time : 2026/2/25 17:48
# @Author : Morton
# @File : scheduledJobs
# @Project : algorithm-engine

from src.util.database import get_redis_client
from src.util.spider import Spider
from src.algorithm.behaviorAnalyzer import recommend
import src.config.application as config
import json
import time


def crawlHotSearch():
    spider = Spider({
        "user-agent": config.USER_AGENT,
        "cookie": config.COOKIE,
        "referer": config.REFERER
    })
    res = spider.crawl(
        f"https://api.bilibili.com/x/web-interface/wbi/search/square?limit=10&platform=web&web_location=333.1007&w_rid=7a4bb9b40d22a2ca4563f32bfccf062b&wts={time.time()}")
    return res


# 爬取B站热搜信息存入Redis数据库
def scheduled_job():
    try:
        res = crawlHotSearch()
        r = get_redis_client()  # 从连接池获取Redis客户端
        r.set("hotSearch", json.dumps(res['data']['trending']))
    except Exception as e:
        print(f"❌ 定时任务执行失败: {e}")


# 定期更新行为标签阈值
def refreshBehaviorThreshold():
    recommend()

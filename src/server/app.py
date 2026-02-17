# _*_ coding : utf-8 _*_
# @Time : 2025/2/19 16:42
# @Author : Morton
# @File : starter
# @Project : recommendation-algorithm

from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from src.algorithm.recommender import getRecommendations
from src.util.spider import Spider
from src.util.database import connectRedis
from src.util.profileBuilder import build_user_profile
from src.util.rabbitmq import startRabbitmq, stopRabbitmq
from src.algorithm.similarUser import startSimilar
import threading
import src.config.application as config
import time
import json
import atexit

app = Flask(__name__)
# 简单的任务队列
profile_tasks = {}
profile_results = {}


# 爬虫代码
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
        r = connectRedis()
        r.set("hotSearch", json.dumps(res['data']['trending']))
    except Exception as e:
        print(f"❌ 定时任务执行失败: {e}")


# 初始化 APScheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_job, trigger="interval", seconds=config.TASK_GAP)
scheduler.start()
print("✅ 定时任务调度器已启动")

# 初始化rabbitmq监听器
rabbitmqListener = startRabbitmq()


@atexit.register
def shutdown():
    """应用关闭时清理资源"""
    print("\n🔄 正在关闭应用...")
    stopRabbitmq()
    scheduler.shutdown()
    print("✅ 应用已关闭")


def startServer():
    print(f"🚀 启动服务器: {config.FLASK_HOST}:{config.FLASK_PORT}")
    app.run(host=config.FLASK_HOST, port=config.FLASK_PORT, debug=False)


@app.route('/recommendations/<int:user_id>', methods=['GET'])
def recommend(user_id):
    # 获取 count 参数，默认为3
    try:
        count = int(request.args.get('count', 3))
        recommendations = getRecommendations(user_id, count)
        return jsonify(recommendations)
    except Exception as e:
        print(f"❌ 推荐接口错误 (用户{user_id}): {e}")
        return jsonify([])


@app.route('/userProfile/<int:user_id>', methods=['GET'])
def buildUserProfile(user_id):
    """
    同步构建用户画像（会阻塞请求直到完成），主要用于测试
    """
    try:
        result = build_user_profile(user_id, save_to_db=True)
        if result['success']:
            return jsonify({'code': 200, 'message': '用户画像构建成功', 'data': None})
        else:
            return jsonify({'code': 500, 'message': '用户画像构建失败', 'data': None})
    except Exception as e:
        print(f"❌ 同步构建画像错误 (用户{user_id}): {e}")
        return jsonify({'code': 500, 'message': f'服务器错误: {str(e)}', 'data': None})


@app.route('/userProfile/<int:user_id>/async', methods=['GET'])
def buildUserProfileAsync(user_id):
    """
    异步构建用户画像（立即返回任务ID，后台执行）
    """
    try:
        task_id = f"{user_id}_{int(time.time())}"

        def background_task():
            try:
                result = build_user_profile(user_id, save_to_db=True)
                profile_results[task_id] = {'status': 'completed', 'result': result}
            except Exception as e:
                profile_results[task_id] = {'status': 'failed', 'error': str(e)}
                print(f"❌ 异步任务失败 (用户{user_id}): {e}")

        # 任务入队
        profile_tasks[task_id] = {'uid': user_id, 'status': 'processing', 'start_time': time.time()}
        thread = threading.Thread(target=background_task)
        thread.daemon = True
        thread.start()
        return jsonify({'code': 200, 'message': '任务已提交', 'data': {'task_id': task_id}})
    except Exception as e:
        print(f"❌ 提交异步任务错误 (用户{user_id}): {e}")
        return jsonify({'code': 500, 'message': f'服务器错误: {str(e)}'})


@app.route('/findSimilar/<int:video_id>/<int:user_id>', methods=['GET'])
def findSimilar(video_id, user_id):
    """
    查找与当前用户相似的用户
    请求示例: /findSimilar/123/456
    """
    try:
        uids = startSimilar(video_id, user_id, 5)
        return jsonify(uids)
    except Exception as e:
        print(f"❌ 查找相似用户失败: {e}")
        return jsonify([])

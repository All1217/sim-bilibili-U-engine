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
from src.util.profileBuilder import build_user_profile, batch_build_profiles
from src.util.rabbitmq import start_rabbitmq_listener, stop_rabbitmq_listener, get_rabbitmq_listener
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
rabbitmq_listener = start_rabbitmq_listener()


@atexit.register
def shutdown():
    """应用关闭时清理资源"""
    print("\n🔄 正在关闭应用...")
    stop_rabbitmq_listener()
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
        return jsonify({'code': 200, 'data': recommendations})
    except Exception as e:
        print(f"❌ 推荐接口错误 (用户{user_id}): {e}")
        return jsonify({'code': 500, 'message': f'服务器错误: {str(e)}'})


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


@app.route('/userProfile/batch', methods=['POST'])
def batchBuildProfiles():
    """
    批量构建用户画像
    请求体: {"uids": [123, 456, 789], "max_workers": 3}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'code': 400, 'message': '请求体不能为空', 'data': None})

        uids = data.get('uids', [])
        max_workers = data.get('max_workers', 3)

        if not uids:
            return jsonify({'code': 400, 'message': '请提供用户ID列表', 'data': None})

        max_workers = min(max_workers, 10)  # 限制最大并发数
        result = batch_build_profiles(uids, save_to_db=True, max_workers=max_workers)
        return jsonify({'code': 200, 'message': '批量构建完成', 'data': result})
    except Exception as e:
        print(f"❌ 批量构建错误: {e}")
        return jsonify({'code': 500, 'message': f'服务器错误: {str(e)}', 'data': None})
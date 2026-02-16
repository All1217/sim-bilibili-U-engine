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
from src.util.profile_builder import build_user_profile, batch_build_profiles
import threading
import src.config.application as config
import time
import json

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
    print("Running scheduled job...")
    res = crawlHotSearch()
    r = connectRedis()
    r.set("hotSearch", json.dumps(res['data']['trending']))
    data = r.get('hotSearch')
    if data:
        hotList = json.loads(data)
        print(hotList)


# 初始化 APScheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_job, trigger="interval", seconds=config.TASK_GAP)
scheduler.start()

def startServer():
    app.run(host=config.FLASK_HOST, port=config.FLASK_PORT, debug=False)

@app.route('/recommendations/<int:user_id>', methods=['GET'])
def recommend(user_id):
    # 获取 count 参数，默认为3
    count = int(request.args.get('count', 3))
    # 使用 user_id 和 count 参数获取推荐内容
    recommendations = getRecommendations(user_id, count)
    return jsonify(recommendations)


@app.route('/userProfile/<int:user_id>', methods=['GET'])
def buildUserProfile(user_id):
    """
    同步构建用户画像（会阻塞请求直到完成），用于测试
    """
    try:
        # 同步执行
        result = build_user_profile(user_id, save_to_db=True)

        if result['success']:
            return jsonify({
                'code': 200,
                'message': '用户画像构建成功',
                'data': {
                    'uid': user_id,
                    'interest_tags': result['interest_tags'],
                    'behavior_tags': result['behavior_tags'],
                    'quality_tags': result['quality_tags'],
                    'execution_time': result['execution_time']
                }
            })
        else:
            return jsonify({
                'code': 500,
                'message': '用户画像构建失败',
                'data': None
            }), 500

    except Exception as e:
        return jsonify({
            'code': 500,
            'message': f'服务器错误: {str(e)}',
            'data': None
        }), 500


@app.route('/userProfile/<int:user_id>/async', methods=['GET'])
def buildUserProfileAsync(user_id):
    """
    异步构建用户画像（立即返回任务ID，后台执行）
    """
    task_id = f"{user_id}_{int(time.time())}"

    def background_task():
        try:
            result = build_user_profile(user_id, save_to_db=True)
            profile_results[task_id] = {
                'status': 'completed',
                'result': result
            }
        except Exception as e:
            profile_results[task_id] = {
                'status': 'failed',
                'error': str(e)
            }

    # 记录任务
    profile_tasks[task_id] = {
        'uid': user_id,
        'status': 'processing',
        'start_time': time.time()
    }

    # 启动后台线程
    thread = threading.Thread(target=background_task)
    thread.daemon = True
    thread.start()

    return jsonify({
        'code': 200,
        'message': '任务已提交',
        'data': {
            'task_id': task_id,
            'uid': user_id,
            'status': 'processing'
        }
    })


@app.route('/userProfile/task/<task_id>', methods=['GET'])
def getTaskStatus(task_id):
    """
    查询异步任务状态
    """
    if task_id in profile_results:
        # 任务已完成
        result = profile_results[task_id]
        if result['status'] == 'completed':
            return jsonify({
                'code': 200,
                'message': '任务已完成',
                'data': {
                    'task_id': task_id,
                    'status': 'completed',
                    'result': result['result']
                }
            })
        else:
            return jsonify({
                'code': 500,
                'message': '任务失败',
                'data': {
                    'task_id': task_id,
                    'status': 'failed',
                    'error': result.get('error')
                }
            })
    elif task_id in profile_tasks:
        # 任务处理中
        return jsonify({
            'code': 202,
            'message': '任务处理中',
            'data': {
                'task_id': task_id,
                'status': 'processing',
                'start_time': profile_tasks[task_id]['start_time']
            }
        })
    else:
        return jsonify({
            'code': 404,
            'message': '任务不存在',
            'data': None
        }), 404


@app.route('/userProfile/batch', methods=['POST'])
def batchBuildProfiles():
    """
    批量构建用户画像
    请求体: {"uids": [123, 456, 789], "max_workers": 3}
    """
    try:
        data = request.get_json()
        uids = data.get('uids', [])
        max_workers = data.get('max_workers', 3)

        if not uids:
            return jsonify({
                'code': 400,
                'message': '请提供用户ID列表',
                'data': None
            }), 400

        # 限制最大并发数
        max_workers = min(max_workers, 10)

        # 执行批量构建
        result = batch_build_profiles(uids, save_to_db=True, max_workers=max_workers)

        return jsonify({
            'code': 200,
            'message': '批量构建完成',
            'data': result
        })

    except Exception as e:
        return jsonify({
            'code': 500,
            'message': f'服务器错误: {str(e)}',
            'data': None
        }), 500

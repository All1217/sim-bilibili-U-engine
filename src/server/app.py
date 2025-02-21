# _*_ coding : utf-8 _*_
# @Time : 2025/2/19 16:42
# @Author : Morton
# @File : starter
# @Project : recommendation-algorithm


from flask import Flask, jsonify, request
from src.algorithm.recommender import getRecommendations

app = Flask(__name__)


@app.route('/recommendations/<int:user_id>', methods=['GET'])
def recommend(user_id):
    # 获取 count 参数，默认为3
    count = int(request.args.get('count', 3))
    # 使用 user_id 和 count 参数获取推荐内容
    recommendations = getRecommendations(user_id, count)
    return jsonify(recommendations)


def startServer():
    app.run(host='0.0.0.0', port=5000, debug=True)

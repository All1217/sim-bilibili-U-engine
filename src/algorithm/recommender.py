# _*_ coding : utf-8 _*_
# @Time : 2025/2/19 16:41
# @Author : Morton
# @File : achieve
# @Project : recommendation-algorithm

import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from src.util.database import connect
import src.config.application as config


def getUserdata():
    """
    从 user_video 表中获取用户与视频的交互行为数据
    """
    conn = connect()
    query = """
    SELECT uid, vid, 
           play AS play_score, 
           love AS like_score, 
           coin AS coin_score, 
           collect AS collect_score
    FROM user_video
    """
    data = pd.read_sql(query, conn)
    conn.close()
    return data


def buildMatrix(data):
    """
    将用户视频交互数据转换为用户-视频评分矩阵
    :param data: 用户视频交互数据 (DataFrame)
    :return: 用户-视频评分矩阵
    """
    # 根据不同权重生成一个评分列
    data["score"] = (
        data["play_score"] * config.PLAY_WEIGHT +
        data["like_score"] * config.LIKE_WEIGHT +
        data["coin_score"] * config.COIN_WEIGHT +
        data["collect_score"] * config.COLLECT_WEIGHT
    )
    # 创建用户-视频评分矩阵
    matrix = data.pivot_table(index="uid", columns="vid", values="score", fill_value=0)
    return matrix


def calSimilarity(matrix):
    """
    计算用户间的相似度 (余弦相似度)
    :param matrix: 用户-视频评分矩阵
    :return: 用户相似度矩阵
    """
    similarity_matrix = cosine_similarity(matrix)
    similarity_df = pd.DataFrame(similarity_matrix, index=matrix.index, columns=matrix.index)
    return similarity_df


def recommend(user_id, matrix, similarity, top_n=5):
    """
    根据用户相似度推荐视频
    :param user_id: 目标用户ID
    :param matrix: 用户-视频评分矩阵
    :param similarity: 用户相似度矩阵
    :param top_n: 推荐视频的数量
    :return: 推荐的视频ID列表
    """
    if user_id not in similarity.index:
        return []
    # 获取与目标用户最相似的其他用户
    similar_users = similarity[user_id].sort_values(ascending=False).index[1:]  # 排除自己
    similar_users_scores = similarity[user_id].sort_values(ascending=False)[1:]

    # 找到目标用户未看过的视频
    unread = set(matrix.loc[user_id][matrix.loc[user_id] > 0].index)
    allVideo = set()  # set()用于创建不含重复元素的集合
    for sim_user in similar_users:
        user_videos = set(matrix.loc[sim_user][matrix.loc[sim_user] > 0].index)
        allVideo.update(user_videos)

    # 推荐用户未看过的视频
    recommended_videos = list(allVideo - unread)

    # 对推荐视频进行评分排序
    video_scores = {}
    for video_id in recommended_videos:
        score = 0
        for sim_user, sim_score in zip(similar_users, similar_users_scores):
            score += matrix.at[sim_user, video_id] * sim_score
        video_scores[video_id] = score

    # 排序并返回前N个视频ID
    sorted_videos = sorted(video_scores.items(), key=lambda x: x[1], reverse=True)
    return [video_id for video_id, _ in sorted_videos[:top_n]]


# 主推荐接口
def getRecommendations(user_id, n):
    """
    对外暴露的推荐接口
    :param user_id: 用户ID
    :param n: 推荐视频数量
    :return: 推荐的 Top N 视频ID列表
    """
    # 1. 加载用户行为数据
    data = getUserdata()
    print("recommender.py:getUserdata()")
    print(data)
    print()
    # 2. 构建用户-视频评分矩阵
    matrix = buildMatrix(data)
    print("recommender.py:buildMatrix(data)")
    print(matrix)
    print()
    # 3. 计算用户相似度矩阵
    similarity = calSimilarity(matrix)
    print("recommender.py:calSimilarity(matrix)")
    print(similarity)
    print()
    # 4. 推荐视频
    videoList = recommend(user_id, matrix, similarity, n)
    print("recommender.py:recommend(user_id, matrix, similarity, n)")
    print(videoList)
    print()
    return videoList

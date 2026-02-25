# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : interestTag.py
# @Project : algorithm-engine
# TODO: matchTextModel()需要批处理优化！原来都是在本地计算的，引入大模型后每条弹幕、每个视频标签都要发一次网络请求，非常慢

from src.util.wordHandler import get_segmenter
from src.util.database import mysql_cursor
from src.util.jsonHandler import loadJson
from collections import defaultdict
from typing import List, Dict, Any
from src.algorithm.vectorization import getTagVectorManager
import datetime

# 初始化分词器
segmenter = get_segmenter(use_stopwords=True, use_pos_filter=True)

# 从JSON文件加载语义标签配置
SEMANTIC_TAGS = loadJson('tags.json')


def matchText(text):
    """
    将文本匹配到语义标签
    返回: {tag_name: 匹配次数}
    """
    if not text:
        return {}
    # 分词并提取关键词
    words = segmenter.segment(text)
    keywords = segmenter.extractKeywords(text, topK=5)
    # 合并所有有效词
    effective_words = set(words + keywords)
    text_lower = text.lower()
    matches = defaultdict(int)
    for tag_name, keywords_list in SEMANTIC_TAGS.items():
        for keyword in keywords_list:
            if keyword.lower() in effective_words or keyword.lower() in text_lower:
                matches[tag_name] += 1
    return dict(matches)


def matchTextModel(text: str, use_vector: bool = True) -> Dict[str, int]:
    """
    标签匹配函数
    Args:
        text: 输入文本
        use_vector: 是否尝试使用向量方法
    Returns:
        {tag_name: match_count, ...} 兼容旧格式
    """
    if not text:
        return {}
    if use_vector:
        try:
            manager = getTagVectorManager()
            similar_tags = manager.search_similar_tags(text, top_k=3)
            matches = {}
            for tag, similarity in similar_tags.items():
                if similarity > 0.5:
                    matches[tag] = max(1, min(10, int(similarity * 20)))
            if matches:
                return matches
        except Exception as e:
            print(f"⚠️ 向量匹配失败: {e}，降级到关键词匹配")
    return matchText(text)


def calInterestTags(uid, use_time_decay=True, normalize=False):
    """
    计算单个用户的兴趣标签
    Args:
        uid: 用户ID
        use_time_decay: 是否使用时间衰减（推荐True）
        normalize: 是否归一化权重到[0,1]
    Returns:
        {tag_name: weight, ...}
    """
    # 初始化权重字典
    weights = {tag: 0 for tag in SEMANTIC_TAGS.keys()}

    # ==================== 1. 从观看视频中提取兴趣 ====================
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT v.vid, v.tags, uv.play, uv.love, uv.coin, uv.collect, uv.play_time
            FROM user_video uv
            JOIN video v ON uv.vid = v.vid
            WHERE uv.uid = %s AND v.status = 1
        """, (uid,))
        user_videos = cursor.fetchall()

    for row in user_videos:
        tags_str = row['tags']
        play = row['play']
        love = row['love']
        coin = row['coin']
        collect = row['collect']
        play_time = row['play_time']
        if not tags_str or not tags_str.strip():
            continue
        video_tags = tags_str.strip().split()
        video_text = " ".join(video_tags)
        tag_matches = matchTextModel(video_text)
        if not tag_matches:
            continue
        # 计算交互权重
        interaction_weight = 0
        if play > 0:
            interaction_weight += 1
        if love == 1:
            interaction_weight += 2
        if coin > 0:
            interaction_weight += coin
        if collect == 1:
            interaction_weight += 3
        # 视频源权重系数
        video_factor = 0.6
        # 时间衰减（如果启用）
        time_factor = 1.0
        if use_time_decay and play_time:
            days_diff = (datetime.datetime.now() - play_time).days
            if days_diff <= 30:
                time_factor = 1.0
            elif days_diff <= 90:
                time_factor = 0.8
            elif days_diff <= 180:
                time_factor = 0.6
            else:
                time_factor = 0.3
        for tag_name, match_count in tag_matches.items():
            weights[tag_name] += interaction_weight * match_count * video_factor * time_factor

    # ==================== 2. 从弹幕中提取兴趣 ====================
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT content, create_date
            FROM danmu
            WHERE uid = %s AND status = 1
        """, (uid,))
        danmaku_rows = cursor.fetchall()

    now = datetime.datetime.now()
    for row in danmaku_rows:
        content = row['content']
        create_date = row['create_date']
        tag_matches = matchTextModel(content)
        if not tag_matches:
            continue
        time_factor = 1.0
        if use_time_decay and create_date:
            days_diff = (now - create_date).days
            if days_diff <= 30:
                time_factor = 1.0
            elif days_diff <= 90:
                time_factor = 0.8
            elif days_diff <= 180:
                time_factor = 0.5
            else:
                time_factor = 0.2
        # 弹幕长度加权
        length = len(content)
        if length <= 5:
            length_factor = 0.5
        elif length <= 15:
            length_factor = 1.0
        else:
            length_factor = 1.5
        # 弹幕权重系数
        danmaku_factor = 1.0
        for tag_name, match_count in tag_matches.items():
            weights[tag_name] += match_count * time_factor * length_factor * danmaku_factor

    # 过滤掉权重为0的标签
    result = {tag: w for tag, w in weights.items() if w > 0}

    if not result:
        return {}

    # 归一化
    if normalize:
        max_w = max(result.values())
        if max_w > 0:
            result = {tag: round(w / max_w, 4) for tag, w in result.items()}

    return result


def saveTags(uid, tags_dict):
    """
    将用户标签保存到数据库
    """
    if not tags_dict:
        return
    try:
        with mysql_cursor() as cursor:
            cursor.execute("DELETE FROM user_tag WHERE uid = %s", (uid,))
            insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
            values = [(uid, tag, weight) for tag, weight in tags_dict.items()]
            cursor.executemany(insert_sql, values)
    except Exception as e:
        print(f"❌ 用户 {uid} 兴趣标签保存失败: {e}")
        raise


def geneInterestTags(uid, use_time_decay=True, normalize=False, auto_save=True):
    """
    构建用户兴趣画像（主函数）
    Args:
        uid: 用户ID
        use_time_decay: 是否使用时间衰减
        normalize: 是否归一化
        auto_save: 是否自动保存到数据库
    Returns:
        {tag_name: weight, ...}
    """
    # 计算标签
    tags = calInterestTags(uid, use_time_decay, normalize)
    if not tags:
        return {}

    # 保存到数据库
    if auto_save:
        saveTags(uid, tags)

    return tags

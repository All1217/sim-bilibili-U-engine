# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : qualityTag.py
# @Project : algorithm-engine

from src.util.jsonHandler import loadJson
from src.util.database import mysql_cursor
from src.algorithm.danmuScore import calDanmuScore, preprocessText
from src.algorithm.danmuScore import PROFESSIONAL_WORDS
import numpy as np

THRESHOLDS = loadJson('qualityThreshold.json')


def loadDanmu(uid):
    """
    加载用户的所有弹幕
    返回: 弹幕字典列表，每条包含(text, create_date, vid, time_point)
    """
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT id, content, create_date, vid, time_point
            FROM danmu
            WHERE uid = %s AND status = 1
            ORDER BY create_date
        """, (uid,))
        rows = cursor.fetchall()

    danmakus = []
    for row in rows:
        danmakus.append({
            'id': row['id'],
            'text': row['content'],
            'create_date': row['create_date'],
            'vid': row['vid'],
            'time_point': row['time_point']
        })
    return danmakus


def getUserProfile(uid):
    """
    获取发送者已有的用户画像
    返回: 包含活跃天数等信息的字典
    """
    with mysql_cursor() as cursor:
        # 获取用户的所有标签
        cursor.execute("""
            SELECT tag_name, weight
            FROM user_tag
            WHERE uid = %s
        """, (uid,))
        tag_rows = cursor.fetchall()
        # 计算活跃天数（基于弹幕发送时间）
        cursor.execute("""
            SELECT DATEDIFF(MAX(create_date), MIN(create_date)) as active_span
            FROM danmu
            WHERE uid = %s AND status = 1
        """, (uid,))
        active_result = cursor.fetchone()
    profile = {}
    for row in tag_rows:
        profile[row['tag_name']] = row['weight']
    active_span = active_result['active_span'] if active_result else None
    if active_span:
        profile['active_days'] = active_span + 1
    else:
        profile['active_days'] = 0
    return profile


def getVideoContext(vid):
    """
    获取弹幕所在的视频上下文
    """
    # 获取视频分区
    with mysql_cursor() as cursor:
        cursor.execute("SELECT mc_id FROM video WHERE vid = %s", (vid,))
        result = cursor.fetchone()
    class VideoContext:
        def __init__(self, zone):
            self.zone = zone
            self.current_viewers = 100
            self.current_frame_keywords = []
        def count_similar(self, text):
            return 0
    return VideoContext(result['mc_id'] if result else None)


def calQualityStats(uid):
    """
    计算用户弹幕的质量统计信息
    返回: (评分列表, 详细统计字典)
    """
    # 1. 加载用户弹幕
    danmakus = loadDanmu(uid)
    if not danmakus:
        return [], {}
    # 2. 获取用户画像（用于评分）
    sender_profile = getUserProfile(uid)
    # 3. 初始化统计
    scores = []
    stats = {
        'total_count': len(danmakus),
        'high_quality': 0,
        'low_quality': 0,
        'professional_count': 0,
        'meme_count': 0,
        'long_count': 0,
        'short_count': 0,
        'total_length': 0,
        'scores': []
    }
    # 4. 逐条处理弹幕
    for i, danmaku in enumerate(danmakus):
        # 获取视频上下文
        context = getVideoContext(danmaku['vid'])
        # 计算单条弹幕评分
        score = calDanmuScore(danmaku, sender_profile, context)
        scores.append(score)
        stats['scores'].append(score)
        # 质量等级统计
        if score >= THRESHOLDS['high_quality_threshold']:
            stats['high_quality'] += 1
        elif score <= THRESHOLDS['low_quality_threshold']:
            stats['low_quality'] += 1
        # 预处理文本
        words = preprocessText(danmaku['text'])
        # 专业词汇判断
        # TODO: 业务重复，考虑移除
        if any(word in PROFESSIONAL_WORDS for word in words):
            stats['professional_count'] += 1
        # 长度统计
        text_len = len(danmaku['text'])
        stats['total_length'] += text_len
        if text_len >= THRESHOLDS['long_danmaku_threshold']:
            stats['long_count'] += 1
        elif text_len <= THRESHOLDS['short_danmaku_threshold']:
            stats['short_count'] += 1
    return scores, stats


def calQualityTags(uid):
    """
    获取用户的质量维度标签
    返回: {tag_name: weight}
    """
    scores, stats = calQualityStats(uid)
    if not scores:
        return {}
    tags = {}
    total = stats['total_count']
    avg_score = np.mean(scores)
    # ===== 1. 总体质量标签 =====
    if avg_score >= THRESHOLDS['high_quality_threshold']:
        tags['高质量弹幕贡献者'] = round(avg_score, 2)
    elif avg_score <= THRESHOLDS['low_quality_threshold']:
        tags['低质量弹幕倾向'] = round(avg_score, 2)
    # ===== 2. 内容类型标签 =====
    # TODO: 上游逻辑失效，建议修改或移除
    if total > 0:
        professional_ratio = stats['professional_count'] / total
        if professional_ratio >= THRESHOLDS['professional_ratio_threshold']:
            tags['干货贡献者'] = round(professional_ratio, 2)
    # ===== 3. 形式特征标签 =====
    if total > 0:
        long_ratio = stats['long_count'] / total
        short_ratio = stats['short_count'] / total
        if long_ratio > 0.3 and long_ratio > short_ratio:
            tags['长文弹幕偏好'] = round(long_ratio, 2)
        elif short_ratio > 0.5:
            tags['短平快弹幕'] = round(short_ratio, 2)
    # ===== 4. 稳定性标签 =====
    if total >= THRESHOLDS['stable_contributor_min']:
        std_dev = np.std(scores)
        stability = 1.0 - min(std_dev, 1.0)
        tags['稳定贡献者'] = round(stability, 2)
    # ===== 5. 极端值标签 =====
    if stats['high_quality'] >= 5:
        tags['精品弹幕制造机'] = stats['high_quality']
    return tags


def saveToDB(uid, tags_dict):
    """
    将用户的质量标签保存到MySQL
    """
    if not tags_dict:
        return
    # 定义质量标签列表（用于删除）
    quality_tags = [
        "高质量弹幕贡献者", "低质量弹幕倾向", "干货贡献者",
        "长文弹幕偏好", "短平快弹幕", "稳定贡献者", "精品弹幕制造机"
    ]
    try:
        with mysql_cursor() as cursor:
            # 删除该用户的质量标签
            placeholders = ','.join(['%s'] * len(quality_tags))
            cursor.execute(f"DELETE FROM user_tag WHERE uid = %s AND tag_name IN ({placeholders})",
                           (uid, *quality_tags))
            # 批量插入新标签
            if tags_dict:
                insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
                values = [(uid, tag, weight) for tag, weight in tags_dict.items()]
                cursor.executemany(insert_sql, values)
    except Exception as e:
        print(f"❌ 用户 {uid} 质量标签保存失败: {e}")
        raise


def geneQualityTags(uid, auto_save=True):
    """
    构建用户质量画像（主函数）
    Args:
        uid: 用户ID
        auto_save: 是否自动保存到数据库
    Returns:
        {tag_name: weight, ...}
    """
    tags = calQualityTags(uid)
    if not tags:
        return {}
    if auto_save:
        saveToDB(uid, tags)
    return tags

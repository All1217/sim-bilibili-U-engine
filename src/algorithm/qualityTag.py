# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : qualityTag.py
# @Project : algorithm-engine

from src.util.jsonHandler import loadJson
from src.util.database import mysql_cursor
from src.util.wordHandler import get_segmenter
from src.algorithm.uniqueness import calculate_uniqueness_scores
import numpy as np
import jieba.analyse
from collections import defaultdict

PROFESSIONAL_WORDS = loadJson('professionalWords.json')
THRESHOLDS = loadJson('qualityThreshold.json')

_segmenter = get_segmenter(use_stopwords=True, use_pos_filter=False)


def preprocessText(text):
    if not text:
        return []
    return _segmenter.segment(text)


def _countProfessionalWords(words):
    return sum(1 for w in words if w in PROFESSIONAL_WORDS)


def calDanmuScore(text, words, video_context, uniqueness_score=None):
    if not text or not words:
        return 0.0

    if uniqueness_score is None:
        uniqueness_score = 1.0

    score = 0.0

    # 1. 文本长度（20%）
    length_score = min(len(words) / 10, 1.0)
    score += length_score * 0.2

    # 2. 内容深度（30%）
    professional_count = _countProfessionalWords(words)
    if len(words) > 0:
        professional_ratio = professional_count / len(words)
        depth_score = 0.3 + professional_ratio * 0.7
        depth_score = min(depth_score, 1.0)
    else:
        depth_score = 0.3
    score += depth_score * 0.3

    # 3. 内容独特性（30%）
    score += uniqueness_score * 0.3

    # 4. 内容相关性（20%）
    clean_text = ' '.join(words)
    keywords = jieba.analyse.extract_tags(clean_text, topK=3)
    if any(keyword in video_context.current_frame_keywords for keyword in keywords):
        score += 0.2

    return min(score, 1.0)


def loadDanmu(uid, preload=None):
    """加载用户的所有弹幕，若传入 preload 则直接使用预加载数据"""
    if preload and 'danmu_list' in preload:
        return preload['danmu_list']

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


def getVideoContext(vid, preload=None):
    """
    获取弹幕所在的视频上下文。
    preload 中包含 'vid_mc_map' 时直接查表，无需访问数据库。
    """
    if preload and 'vid_mc_map' in preload:
        zone = preload['vid_mc_map'].get(vid, None)
    else:
        with mysql_cursor() as cursor:
            cursor.execute("SELECT mc_id FROM video WHERE vid = %s", (vid,))
            result = cursor.fetchone()
        zone = result['mc_id'] if result else None

    class VideoContext:
        def __init__(self, zone):
            self.zone = zone
            self.current_viewers = 100
            self.current_frame_keywords = []

        def count_similar(self, text):
            return 0

    return VideoContext(zone)


def calQualityStats(uid, preload=None):
    danmakus = loadDanmu(uid, preload)
    if not danmakus:
        return [], {}

    # ===== 分词并按 vid 分组 =====
    vid_groups = defaultdict(list)
    for idx, danmaku in enumerate(danmakus):
        text = danmaku['text']
        words = preprocessText(text)
        danmaku['_words'] = words
        danmaku['_clean_text'] = ' '.join(words)
        vid_groups[danmaku['vid']].append(idx)

    # ===== 按视频分组计算独特性得分 =====
    uniqueness_map = {}
    for vid, indices in vid_groups.items():
        group_texts = [danmakus[i]['_clean_text'] for i in indices]
        group_scores = calculate_uniqueness_scores(group_texts)
        for i, score in zip(indices, group_scores):
            uniqueness_map[i] = score

    # ===== 逐条评分与统计 =====
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

    for idx, danmaku in enumerate(danmakus):
        text = danmaku['text']
        words = danmaku['_words']
        context = getVideoContext(danmaku['vid'], preload)
        uniqueness_score = uniqueness_map.get(idx, 1.0)

        score = calDanmuScore(text, words, context, uniqueness_score)
        scores.append(score)
        stats['scores'].append(score)

        if score >= THRESHOLDS['high_quality_threshold']:
            stats['high_quality'] += 1
        elif score <= THRESHOLDS['low_quality_threshold']:
            stats['low_quality'] += 1

        if any(w in PROFESSIONAL_WORDS for w in words):
            stats['professional_count'] += 1

        text_len = len(text)
        stats['total_length'] += text_len
        if text_len >= THRESHOLDS['long_danmaku_threshold']:
            stats['long_count'] += 1
        elif text_len <= THRESHOLDS['short_danmaku_threshold']:
            stats['short_count'] += 1

    return scores, stats


def calQualityTags(uid, preload=None):
    scores, stats = calQualityStats(uid, preload)
    if not scores:
        return {}

    tags = {}
    total = stats['total_count']
    avg_score = np.mean(scores)

    if avg_score >= THRESHOLDS['high_quality_threshold']:
        tags['高质量弹幕贡献者'] = round(avg_score, 2)
    elif avg_score <= THRESHOLDS['low_quality_threshold']:
        tags['低质量弹幕倾向'] = round(avg_score, 2)

    if total > 0:
        professional_ratio = stats['professional_count'] / total
        if professional_ratio >= THRESHOLDS['professional_ratio_threshold']:
            tags['干货贡献者'] = round(professional_ratio, 2)

    if total > 0:
        long_ratio = stats['long_count'] / total
        short_ratio = stats['short_count'] / total
        if long_ratio > 0.3 and long_ratio > short_ratio:
            tags['长文弹幕偏好'] = round(long_ratio, 2)
        elif short_ratio > 0.5:
            tags['短平快弹幕'] = round(short_ratio, 2)

    if total >= THRESHOLDS['stable_contributor_min']:
        std_dev = np.std(scores)
        stability = 1.0 - min(std_dev, 1.0)
        tags['稳定贡献者'] = round(stability, 2)

    if stats['high_quality'] >= 5:
        tags['精品弹幕制造机'] = stats['high_quality']

    return tags


def geneQualityTags(uid, preload=None):
    """构建用户质量画像（主函数）"""
    tags = calQualityTags(uid, preload)
    return tags if tags else {}
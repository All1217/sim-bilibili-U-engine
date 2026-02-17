# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : qualityTag.py (精简打印版)
# @Project : recommendation-algorithm

from src.util.database import connectMySql
from src.algorithm.danmuScore import calDanmuScore, preprocess_danmaku_text
from src.util.wordHandler import get_segmenter
from src.algorithm.danmuScore import PROFESSIONAL_WORDS
import numpy as np

# 初始化分词器
segmenter = get_segmenter(use_stopwords=True)


def getDBConn():
    """获取数据库连接"""
    return connectMySql()


# ==================== 质量标签阈值配置 ====================
THRESHOLDS = {
    # 高质量弹幕：平均分 ≥ 0.7
    "high_quality_threshold": 0.7,
    # 低质量弹幕：平均分 ≤ 0.3
    "low_quality_threshold": 0.3,
    # 干货贡献者：专业词汇比例 ≥ 20%
    "professional_ratio_threshold": 0.2,
    # 稳定贡献者：至少发过20条弹幕
    "stable_contributor_min": 20,
    # 弹幕长度统计：长弹幕阈值（字符数）
    "long_danmaku_threshold": 20,
    # 短弹幕阈值
    "short_danmaku_threshold": 5
}


# ==================== 核心计算函数 ====================

def load_user_danmakus(uid):
    """
    加载用户的所有弹幕
    返回: 弹幕字典列表，每条包含(text, create_date, vid, time_point)
    """
    conn = getDBConn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, content, create_date, vid, time_point
        FROM danmu
        WHERE uid = %s AND status = 1
        ORDER BY create_date
    """, (uid,))

    danmakus = []
    for row in cursor.fetchall():
        danmakus.append({
            'id': row[0],
            'text': row[1],
            'create_date': row[2],
            'vid': row[3],
            'time_point': row[4]
        })

    cursor.close()
    conn.close()
    return danmakus


def get_user_profile(uid):
    """
    获取发送者的用户画像（从user_tag表）
    返回: 包含活跃天数等信息的字典（移除了点赞相关）
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 获取用户的所有标签（用于后续扩展）
    cursor.execute("""
        SELECT tag_name, weight
        FROM user_tag
        WHERE uid = %s
    """, (uid,))

    profile = {}
    for tag_name, weight in cursor.fetchall():
        profile[tag_name] = weight

    # 计算活跃天数（基于弹幕发送时间）
    cursor.execute("""
        SELECT DATEDIFF(MAX(create_date), MIN(create_date)) as active_span
        FROM danmu
        WHERE uid = %s AND status = 1
    """, (uid,))

    active_span = cursor.fetchone()[0]
    if active_span:
        profile['active_days'] = active_span + 1  # +1是因为跨度从0开始算
    else:
        profile['active_days'] = 0

    cursor.close()
    conn.close()

    return profile


def get_video_context(vid):
    """
    获取弹幕所在的视频上下文（简化版）
    """
    # 获取视频分区
    conn = getDBConn()
    cursor = conn.cursor()
    cursor.execute("SELECT mc_id FROM video WHERE vid = %s", (vid,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    # 返回简化上下文对象
    class VideoContext:
        def __init__(self, zone):
            self.zone = zone
            self.current_viewers = 100  # 默认值
            self.current_frame_keywords = []

        def count_similar(self, text):
            # 简化：假设没有相似弹幕
            return 0

    return VideoContext(result[0] if result else None)


def calculate_quality_stats(uid):
    """
    计算用户弹幕的质量统计信息
    返回: (评分列表, 详细统计字典)
    """
    # 1. 加载用户弹幕
    danmakus = load_user_danmakus(uid)
    if not danmakus:
        return [], {}

    # 2. 获取用户画像（用于评分）
    sender_profile = get_user_profile(uid)

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
        context = get_video_context(danmaku['vid'])

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
        words = preprocess_danmaku_text(danmaku['text'])

        # 专业词汇判断
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


def get_quality_tags(uid):
    """
    获取用户的质量维度标签
    返回: {tag_name: weight}
    """
    scores, stats = calculate_quality_stats(uid)

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


def save_user_tags(uid, tags_dict):
    """
    将用户的质量标签保存到MySQL
    """
    if not tags_dict:
        return

    conn = getDBConn()
    cursor = conn.cursor()

    # 定义质量标签列表（用于删除）
    quality_tags = [
        "高质量弹幕贡献者", "低质量弹幕倾向", "干货贡献者",
        "长文弹幕偏好", "短平快弹幕", "稳定贡献者", "精品弹幕制造机"
    ]
    placeholders = ','.join(['%s'] * len(quality_tags))

    # 只删除该用户的质量标签
    cursor.execute(f"DELETE FROM user_tag WHERE uid = %s AND tag_name IN ({placeholders})",
                   (uid, *quality_tags))

    # 批量插入新标签
    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
    values = [(uid, tag, weight) for tag, weight in tags_dict.items()]

    if values:
        cursor.executemany(insert_sql, values)

    conn.commit()
    cursor.close()
    conn.close()


def build_user_quality_profile(uid, auto_save=True):
    """
    构建用户质量画像（主函数）

    Args:
        uid: 用户ID
        auto_save: 是否自动保存到数据库

    Returns:
        {tag_name: weight, ...}
    """
    # 计算质量标签
    tags = get_quality_tags(uid)

    if not tags:
        return {}

    # 保存到数据库
    if auto_save:
        save_user_tags(uid, tags)

    return tags


def update_thresholds(new_thresholds):
    """
    更新阈值配置（支持动态调整）
    """
    global THRESHOLDS
    THRESHOLDS.update(new_thresholds)
    return THRESHOLDS


# ==================== 测试代码 ====================
if __name__ == "__main__":
    import sys

    # 从命令行参数获取用户ID
    if len(sys.argv) > 1:
        test_uid = int(sys.argv[1])
    else:
        # 默认测试用户
        test_uid = 123123123

    # 构建用户质量画像
    tags = build_user_quality_profile(
        uid=test_uid,
        auto_save=True
    )

    if tags:
        print(f"用户 {test_uid} 质量标签: {dict(list(tags.items())[:3])}...")
    else:
        print(f"用户 {test_uid} 无质量标签")
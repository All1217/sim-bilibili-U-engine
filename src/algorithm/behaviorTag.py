# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : behaviorTag.py
# @Project : algorithm-engine

from src.util.database import mysql_cursor, get_redis_client
from src.common.redisConstants import BEHAVIOR_THRESHOLD_KEY
from src.util.jsonHandler import loadJson


def loadThreshold():
    """
    加载阈值配置
    返回: 阈值字典
    """
    # 尝试从Redis获取
    try:
        redis_client = get_redis_client()
        temp = redis_client.hgetall(BEHAVIOR_THRESHOLD_KEY)
        if temp:
            thresholds = {
                'active_threshold': float(temp.get('active_threshold', 0)),
                'passive_threshold': float(temp.get('passive_threshold', 0)),
                'night_ratio_threshold': float(temp.get('night_ratio_threshold', 0)),
                'night_min_samples': int(temp.get('night_min_samples', 0)),
                'like_ratio_threshold': float(temp.get('like_ratio_threshold', 0.2)),
                'collect_threshold': int(temp.get('collect_threshold', 10))
            }
            return thresholds
    except Exception as e:
        print(f'从Redis获取阈值失败: {e}')

    # 如果Redis没有，再到本地找
    try:
        config = loadJson("recommend.json")
        thresholds = {
            'active_threshold': config['active_threshold'],
            'passive_threshold': config['passive_threshold'],
            'night_ratio_threshold': config['night_ratio_threshold'],
            'night_min_samples': config['night_min_samples'],
            'like_ratio_threshold': config.get('like_ratio_threshold', 0.2),
            'collect_threshold': config.get('collect_threshold', 10)
        }
        return thresholds
    except Exception as e:
        print(f'加载阈值失败，使用默认值: {e}')
        return {
            'active_threshold': 50,
            'passive_threshold': 5,
            'night_ratio_threshold': 0.3,
            'night_min_samples': 10,
            'like_ratio_threshold': 0.2,
            'collect_threshold': 10
        }


# 全局阈值（模块加载时初始化）
THRESHOLDS = loadThreshold()


def calActiveLevel(uid):
    """
    判断用户的互动活跃程度
    返回: "互动积极分子" 或 "潜水观望者" 或 None（中等活跃不标记）
    """
    if THRESHOLDS is None:
        return None
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) as danmaku_count
            FROM danmu
            WHERE uid = %s AND status = 1
        """, (uid,))
        result = cursor.fetchone()
        count = result['danmaku_count'] if result else 0
    if count >= THRESHOLDS["active_threshold"]:
        return "互动积极分子"
    elif count <= THRESHOLDS["passive_threshold"]:
        return "潜水观望者"
    else:
        return None


def isNightOwl(uid):
    """
    判断用户是否为夜猫子型（偏好凌晨活动）
    返回: "夜猫子型用户" 或 None
    """
    if THRESHOLDS is None:
        return None

    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT HOUR(create_date) as hour
            FROM danmu
            WHERE uid = %s AND status = 1
        """, (uid,))
        rows = cursor.fetchall()
        hours = [row['hour'] for row in rows]

    if len(hours) < THRESHOLDS["night_min_samples"]:
        return None  # 样本太少，不判断
    # 统计凌晨时段（0点-5点）的弹幕比例
    night_hours = [h for h in hours if 0 <= h <= 5]
    night_ratio = len(night_hours) / len(hours) if hours else 0
    if night_ratio >= THRESHOLDS["night_ratio_threshold"]:
        return "夜猫子型用户"
    else:
        return None


def getOneUserBehaviorTags(uid, include_extended=False):
    """
    获取单个用户的所有行为标签
    Args:
        uid: 用户ID
        include_extended: 是否包含扩展标签（点赞狂魔、收藏家）

    返回: {tag_name: weight}  weight固定为1表示有该标签
    """
    tags = {}
    # 1. 活跃程度
    activity = calActiveLevel(uid)
    if activity:
        tags[activity] = 1
    # 2. 夜猫子
    night = isNightOwl(uid)
    if night:
        tags[night] = 1
    return tags


def saveToDB(uid, tags_dict):
    """
    将用户的行为标签保存到MySQL
    Args:
        uid: 用户ID
        tags_dict: {tag_name: weight, ...}
    """
    if not tags_dict:
        return
    # 定义行为标签列表（用于删除）
    behavior_tags = ["互动积极分子", "潜水观望者", "夜猫子型用户", "点赞狂魔", "收藏家"]
    try:
        with mysql_cursor() as cursor:
            # 只删除该用户的行为标签（保留兴趣标签和质量标签）
            placeholders = ','.join(['%s'] * len(behavior_tags))
            cursor.execute(f"DELETE FROM user_tag WHERE uid = %s AND tag_name IN ({placeholders})",
                           (uid, *behavior_tags))
            # 批量插入新标签
            if tags_dict:
                insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
                values = [(uid, tag, weight) for tag, weight in tags_dict.items()]
                cursor.executemany(insert_sql, values)
    except Exception as e:
        print(f"❌ 用户 {uid} 行为标签保存失败: {e}")
        raise


def geneBehaviorTags(uid, include_extended=False, auto_save=True):
    """
    构建用户行为画像（主函数）
    Args:
        uid: 用户ID
        include_extended: 是否包含扩展标签
        auto_save: 是否自动保存到数据库
    Returns:
        {tag_name: weight, ...}
    """
    # 计算标签
    tags = getOneUserBehaviorTags(uid, include_extended)
    if not tags:
        return {}
    # 保存到数据库
    if auto_save:
        saveToDB(uid, tags)
    return tags

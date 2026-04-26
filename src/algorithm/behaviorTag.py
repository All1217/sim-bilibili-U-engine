# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : behaviorTag.py
# @Project : algorithm-engine

from src.util.database import mysql_cursor, get_redis_client
from src.common.redisConstants import BEHAVIOR_THRESHOLD_KEY
from src.util.jsonHandler import loadJson


def loadThreshold():
    try:
        redis_client = get_redis_client()
        temp = redis_client.hgetall(BEHAVIOR_THRESHOLD_KEY)
        if temp:
            return {
                'active_threshold': float(temp.get('active_threshold', 0)),
                'passive_threshold': float(temp.get('passive_threshold', 0)),
                'night_ratio_threshold': float(temp.get('night_ratio_threshold', 0)),
                'night_min_samples': int(temp.get('night_min_samples', 0)),
                'like_ratio_threshold': float(temp.get('like_ratio_threshold', 0.2)),
                'collect_threshold': int(temp.get('collect_threshold', 10))
            }
    except Exception as e:
        print(f'从Redis获取阈值失败: {e}')

    try:
        config = loadJson("recommend.json")
        return {
            'active_threshold': config['active_threshold'],
            'passive_threshold': config['passive_threshold'],
            'night_ratio_threshold': config['night_ratio_threshold'],
            'night_min_samples': config['night_min_samples'],
            'like_ratio_threshold': config.get('like_ratio_threshold', 0.2),
            'collect_threshold': config.get('collect_threshold', 10)
        }
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


THRESHOLDS = loadThreshold()


def calActiveLevel(uid, preload=None):
    """
    判断用户的互动活跃程度
    Args:
        preload: 预加载数据，包含 'danmu_count'
    """
    if THRESHOLDS is None:
        return None

    if preload and 'danmu_count' in preload:
        count = preload['danmu_count']
    else:
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


def isNightOwl(uid, preload=None):
    """
    判断用户是否为夜猫子型（偏好凌晨活动）
    Args:
        preload: 预加载数据，包含 'danmu_hours'（0-23 的整数列表）
    """
    if THRESHOLDS is None:
        return None

    if preload and 'danmu_hours' in preload:
        hours = preload['danmu_hours']
    else:
        with mysql_cursor() as cursor:
            cursor.execute("""
                SELECT HOUR(create_date) as hour
                FROM danmu
                WHERE uid = %s AND status = 1
            """, (uid,))
            rows = cursor.fetchall()
        hours = [row['hour'] for row in rows]

    if len(hours) < THRESHOLDS["night_min_samples"]:
        return None

    night_hours = [h for h in hours if 0 <= h <= 5]
    night_ratio = len(night_hours) / len(hours) if hours else 0

    if night_ratio >= THRESHOLDS["night_ratio_threshold"]:
        return "夜猫子型用户"
    else:
        return None


def getOneUserBehaviorTags(uid, include_extended=False, preload=None):
    tags = {}
    activity = calActiveLevel(uid, preload)
    if activity:
        tags[activity] = 1
    night = isNightOwl(uid, preload)
    if night:
        tags[night] = 1
    return tags


def geneBehaviorTags(uid, include_extended=False, preload=None):
    """
    构建用户行为画像（主函数）
    """
    tags = getOneUserBehaviorTags(uid, include_extended, preload)
    return tags if tags else {}
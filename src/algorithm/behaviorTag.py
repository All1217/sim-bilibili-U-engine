# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : behaviorTag.py (精简打印版)
# @Project : recommendation-algorithm

from src.util.database import connectMySql, connectRedis
from src.common.redisConstants import BEHAVIOR_THRESHOLD_KEY
from src.util.jsonHandler import loadJson


def getDBConn():
    """获取数据库连接"""
    return connectMySql()


def getRedisConn():
    """获取Redis连接"""
    return connectRedis()


def load_thresholds():
    """
    加载阈值配置（优先从Redis，降级到本地JSON）
    返回: 阈值字典
    """
    # 尝试从Redis获取
    try:
        redisConn = getRedisConn()
        temp = redisConn.hgetall(BEHAVIOR_THRESHOLD_KEY)
        if temp:
            thresholds = {
                'active_threshold': float(
                    temp.get(b'active_threshold', 0) if isinstance(temp.get(b'active_threshold'), bytes) else temp.get(
                        'active_threshold', 0)),
                'passive_threshold': float(
                    temp.get(b'passive_threshold', 0) if isinstance(temp.get(b'passive_threshold'),
                                                                    bytes) else temp.get('passive_threshold', 0)),
                'night_ratio_threshold': float(
                    temp.get(b'night_ratio_threshold', 0) if isinstance(temp.get(b'night_ratio_threshold'),
                                                                        bytes) else temp.get('night_ratio_threshold',
                                                                                             0)),
                'night_min_samples': int(temp.get(b'night_min_samples', 0) if isinstance(temp.get(b'night_min_samples'),
                                                                                         bytes) else temp.get(
                    'night_min_samples', 0)),
                'like_ratio_threshold': float(
                    temp.get(b'like_ratio_threshold', 0.2) if isinstance(temp.get(b'like_ratio_threshold'),
                                                                         bytes) else temp.get('like_ratio_threshold',
                                                                                              0.2)),
                'collect_threshold': int(
                    temp.get(b'collect_threshold', 10) if isinstance(temp.get(b'collect_threshold'),
                                                                     bytes) else temp.get('collect_threshold', 10))
            }
            return thresholds
    except Exception as e:
        print(f'从Redis获取阈值失败: {e}')

    # 降级到本地JSON文件
    try:
        json_config = loadJson("../../Assets/recommend.json")
        # 假设JSON中有behavior_thresholds字段
        if 'behavior_thresholds' in json_config:
            thresholds = json_config['behavior_thresholds']
        else:
            # 使用默认值
            thresholds = {
                'active_threshold': 50,
                'passive_threshold': 5,
                'night_ratio_threshold': 0.3,
                'night_min_samples': 10,
                'like_ratio_threshold': 0.2,
                'collect_threshold': 10
            }
        return thresholds
    except Exception as e:
        print(f'加载阈值失败，使用默认值: {e}')
        # 最终默认值
        return {
            'active_threshold': 50,
            'passive_threshold': 5,
            'night_ratio_threshold': 0.3,
            'night_min_samples': 10,
            'like_ratio_threshold': 0.2,
            'collect_threshold': 10
        }


# 全局阈值（模块加载时初始化）
THRESHOLDS = load_thresholds()


def calActiveLevel(uid):
    """
    判断用户的互动活跃程度
    返回: "互动积极分子" 或 "潜水观望者" 或 None（中等活跃不标记）
    """
    if THRESHOLDS is None:
        return None

    conn = getDBConn()
    cursor = conn.cursor()

    # 统计用户弹幕总数
    cursor.execute("""
        SELECT COUNT(*) as danmaku_count
        FROM danmu
        WHERE uid = %s AND status = 1
    """, (uid,))

    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if count >= THRESHOLDS["active_threshold"]:
        return "互动积极分子"
    elif count <= THRESHOLDS["passive_threshold"]:
        return "潜水观望者"
    else:
        return None  # 中等活跃，不贴标签


def isNightOwl(uid):
    """
    判断用户是否为夜猫子型（偏好凌晨活动）
    返回: "夜猫子型用户" 或 None
    """
    if THRESHOLDS is None:
        return None

    conn = getDBConn()
    cursor = conn.cursor()

    # 获取用户所有弹幕的小时数
    cursor.execute("""
        SELECT HOUR(create_date) as hour
        FROM danmu
        WHERE uid = %s AND status = 1
    """, (uid,))

    hours = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    if len(hours) < THRESHOLDS["night_min_samples"]:
        return None  # 样本太少，不判断

    # 统计凌晨时段（0点-5点）的弹幕比例
    night_hours = [h for h in hours if 0 <= h <= 5]
    night_ratio = len(night_hours) / len(hours) if hours else 0

    if night_ratio >= THRESHOLDS["night_ratio_threshold"]:
        return "夜猫子型用户"
    else:
        return None


def getLikeRatio(uid):
    """
    判断是否为点赞狂魔
    返回: "点赞狂魔" 或 None
    """
    if THRESHOLDS is None:
        return None

    conn = getDBConn()
    cursor = conn.cursor()

    # 统计用户点赞数和播放数
    cursor.execute("""
        SELECT 
            COUNT(*) as total_videos,
            SUM(CASE WHEN love = 1 THEN 1 ELSE 0 END) as like_count
        FROM user_video
        WHERE uid = %s
    """, (uid,))

    result = cursor.fetchone()
    total = result[0] if result else 0
    likes = result[1] if result else 0

    cursor.close()
    conn.close()

    if total < 5:  # 样本太少
        return None

    like_ratio = likes / total if total > 0 else 0
    if like_ratio >= THRESHOLDS.get("like_ratio_threshold", 0.2):
        return "点赞狂魔"
    return None


def getCollectorLevel(uid):
    """
    判断是否为收藏家
    返回: "收藏家" 或 None
    """
    if THRESHOLDS is None:
        return None

    conn = getDBConn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) as collect_count
        FROM user_video
        WHERE uid = %s AND collect = 1
    """, (uid,))

    collect_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if collect_count >= THRESHOLDS.get("collect_threshold", 10):
        return "收藏家"
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

    # 1. 活跃程度（必选）
    activity = calActiveLevel(uid)
    if activity:
        tags[activity] = 1

    # 2. 夜猫子（必选）
    night = isNightOwl(uid)
    if night:
        tags[night] = 1

    # 3. 扩展标签（可选）
    if include_extended:
        like_master = getLikeRatio(uid)
        if like_master:
            tags[like_master] = 1

        collector = getCollectorLevel(uid)
        if collector:
            tags[collector] = 1

    return tags


def save_user_tags(uid, tags_dict):
    """
    将用户的行为标签保存到MySQL

    Args:
        uid: 用户ID
        tags_dict: {tag_name: weight, ...}
    """
    if not tags_dict:
        return

    conn = getDBConn()
    cursor = conn.cursor()

    # 定义行为标签列表（用于删除）
    behavior_tags = ["互动积极分子", "潜水观望者", "夜猫子型用户", "点赞狂魔", "收藏家"]
    placeholders = ','.join(['%s'] * len(behavior_tags))

    # 只删除该用户的行为标签（保留兴趣标签和质量标签）
    cursor.execute(f"DELETE FROM user_tag WHERE uid = %s AND tag_name IN ({placeholders})",
                   (uid, *behavior_tags))

    # 批量插入新标签
    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
    values = [(uid, tag, weight) for tag, weight in tags_dict.items()]

    if values:
        cursor.executemany(insert_sql, values)

    conn.commit()
    cursor.close()
    conn.close()


def build_user_behavior_profile(uid, include_extended=False, auto_save=True):
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
        save_user_tags(uid, tags)

    return tags


def reload_thresholds():
    """
    重新加载阈值配置（可用于动态更新）
    """
    global THRESHOLDS
    THRESHOLDS = load_thresholds()
    return THRESHOLDS


# ==================== 测试代码 ====================
if __name__ == "__main__":
    import sys

    # 方式1：从命令行参数获取用户ID
    if len(sys.argv) > 1:
        test_uid = int(sys.argv[1])
    else:
        # 方式2：硬编码测试用户
        test_uid = 123123123

    # 构建用户画像（包含扩展标签，自动保存）
    tags = build_user_behavior_profile(
        uid=test_uid,
        include_extended=True,
        auto_save=True
    )

    if tags:
        print(f"用户 {test_uid} 行为标签: {list(tags.keys())}")
    else:
        print(f"用户 {test_uid} 无行为标签")
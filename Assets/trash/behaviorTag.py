# _*_ coding : utf-8 _*_
# @Time : 2026/2/14 20:37
# @Author : Morton
# @File : behaviorTag
# @Project : recommendation-algorithm

from src.util.database import connectMySql, connectRedis
from src.common.redisConstants import BEHAVIOR_THRESHOLD_KEY
from src.util.jsonHandler import loadJson


# 获取数据库连接
def getDBConn():
    return connectMySql()
def getRedisConn():
    return connectRedis()

# 获取判断阈值
THRESHOLDS = None
try:
    print('正在尝试从Redis获取阈值……')
    redisConn = getRedisConn()
    temp = redisConn.hgetall(BEHAVIOR_THRESHOLD_KEY)
    THRESHOLDS = {
                'active_threshold': float(temp.get('active_threshold', 0)),
                'passive_threshold': float(temp.get('passive_threshold', 0)),
                'night_ratio_threshold': float(temp.get('night_ratio_threshold', 0)),
                'night_min_samples': int(temp.get('night_min_samples', 0))
    }
except Exception as outE:
    print('从Redis获取阈值配置失败，正常尝试从本地JSON文件获取配置……')
    try:
        THRESHOLDS = loadJson("../recommend.json")
    except FileNotFoundError as innerE:
        print('获取配置失败！')
        print(innerE)


def calActiveLevel(uid):
    """
    判断用户的互动活跃程度
    返回: "互动积极分子" 或 "潜水观望者" 或 None（中等活跃不标记）
    """
    if THRESHOLDS is None:
        print('缺少建议阈值配置信息！')
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
        print('缺少建议阈值配置信息！')
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
    night_ratio = len(night_hours) / len(hours)

    if night_ratio >= THRESHOLDS["night_ratio_threshold"]:
        return "夜猫子型用户"
    else:
        return None


def getLikeRatio(uid):
    """
    扩展功能：判断是否为点赞狂魔
    返回: "点赞狂魔" 或 None
    """
    if THRESHOLDS is None:
        print('缺少建议阈值配置信息！')
        return None
    conn = getDBConn()
    cursor = conn.cursor()

    # 统计用户点赞数和播放数
    cursor.execute("""
        SELECT 
            COUNT(*) as total_videos,
            SUM(love = 1) as like_count
        FROM user_video
        WHERE uid = %s
    """, (uid,))

    total, likes = cursor.fetchone()
    cursor.close()
    conn.close()

    if total < 5:  # 样本太少
        return None

    like_ratio = likes / total if total > 0 else 0
    if like_ratio >= THRESHOLDS["like_ratio_threshold"]:
        return "点赞狂魔"
    return None


def getCollectorLevel(uid):
    """
    扩展功能：判断是否为收藏家
    返回: "收藏家" 或 None
    """
    if THRESHOLDS is None:
        print('缺少建议阈值配置信息！')
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

    if collect_count >= THRESHOLDS["collect_threshold"]:
        return "收藏家"
    return None


def getOneUserBehaviorTags(uid):
    """
    获取单个用户的所有行为标签
    返回: {tag_name: weight}  weight固定为1表示有该标签
    """
    tags = {}

    # 活跃程度
    activity = calActiveLevel(uid)
    if activity:
        tags[activity] = 1

    # 夜猫子
    night = isNightOwl(uid)
    if night:
        tags[night] = 1

    # 扩展标签（取消注释即可启用）
    # like_master = get_like_ratio(uid)
    # if like_master:
    #     tags[like_master] = 1
    #
    # collector = get_collector_level(uid)
    # if collector:
    #     tags[collector] = 1

    return tags


def calAllUserBehavior():
    """
    计算所有正常用户的行为标签
    返回: {uid: {tag_name: weight}}
    """
    conn = getDBConn()
    cursor = conn.cursor()
    cursor.execute("SELECT uid FROM user WHERE status = 0")
    users = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    all_tags = {}
    for uid in users:
        tags = getOneUserBehaviorTags(uid)
        if tags:  # 只保存有标签的用户
            all_tags[uid] = tags

    return all_tags


# ==================== 保存函数 ====================

def saveOneUserTags(uid, tags_dict):
    """
    将用户的行为标签保存到MySQL
    :param uid: 用户ID
    :param tags_dict: {tag_name: weight, ...}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 删除该用户的旧行为标签（避免与语义标签混淆，只删除行为相关的）
    behavior_tags = ["互动积极分子", "潜水观望者", "夜猫子型用户", "点赞狂魔", "收藏家"]
    placeholders = ','.join(['%s'] * len(behavior_tags))
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


def saveAllTags(data):
    """
    批量保存所有用户的行为标签
    :param data: {uid: {tag_name: weight, ...}}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 清空行为标签（只删除行为相关的）
    behavior_tags = ["互动积极分子", "潜水观望者", "夜猫子型用户", "点赞狂魔", "收藏家"]
    placeholders = ','.join(['%s'] * len(behavior_tags))
    cursor.execute(f"DELETE FROM user_tag WHERE tag_name IN ({placeholders})", behavior_tags)

    # 批量插入
    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
    values = []
    for uid, tags in data.items():
        for tag, weight in tags.items():
            values.append((uid, tag, weight))

    if values:
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()

    cursor.close()
    conn.close()
    print(f"已保存 {len(values)} 条用户行为标签记录")


if __name__ == "__main__":
    print(THRESHOLDS)

    print("\n正在计算所有用户的行为标签...")
    user_tags = calAllUserBehavior()

    # 统计各标签数量
    tag_stats = {}
    for uid, tags in user_tags.items():
        for tag in tags:
            tag_stats[tag] = tag_stats.get(tag, 0) + 1

    print("\n各行为标签用户数：")
    for tag, count in tag_stats.items():
        print(f"  {tag}: {count} 人")

    # 打印前10个用户的标签（示例）
    print("\n前10个用户的标签示例：")
    for i, (uid, tags) in enumerate(user_tags.items()):
        if i >= 10:
            break
        tag_list = list(tags.keys())
        print(f"用户 {uid}: {', '.join(tag_list)}")

    # 保存到数据库（取消注释即可启用）
    print("\n正在保存到数据库...")
    saveAllTags(user_tags)
    print("完成！")

# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : similarUser.py (精简版)
# @Project : recommendation-algorithm

from src.util.database import connectMySql
import math
from collections import defaultdict


def getDBConn():
    return connectMySql()


def get_user_tags(uid):
    """
    获取指定用户的所有标签及其权重
    返回: {tag_name: weight, ...}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tag_name, weight
        FROM user_tag
        WHERE uid = %s
    """, (uid,))

    tags = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.close()
    conn.close()
    return tags


def get_video_danmaku_users(vid, exclude_uid=None, min_danmaku=1):
    """
    获取指定视频中发了弹幕的用户ID列表
    """
    conn = getDBConn()
    cursor = conn.cursor()

    query = """
        SELECT uid, COUNT(*) as cnt
        FROM danmu
        WHERE vid = %s AND status = 1
    """
    params = [vid]

    if exclude_uid:
        query += " AND uid != %s"
        params.append(exclude_uid)

    query += " GROUP BY uid HAVING cnt >= %s ORDER BY cnt DESC"
    params.append(min_danmaku)

    cursor.execute(query, params)
    users = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return users


def cosine_similarity(tags1, tags2):
    """
    计算两个用户标签的余弦相似度
    """
    if not tags1 or not tags2:
        return 0

    common_tags = set(tags1.keys()) & set(tags2.keys())
    if not common_tags:
        return 0

    dot_product = sum(tags1[tag] * tags2[tag] for tag in common_tags)
    norm1 = math.sqrt(sum(w ** 2 for w in tags1.values()))
    norm2 = math.sqrt(sum(w ** 2 for w in tags2.values()))

    if norm1 == 0 or norm2 == 0:
        return 0

    return dot_product / (norm1 * norm2)


def find_similar_users(target_uid, vid, top_n=10, min_danmaku=1):
    """
    查找与目标用户最相似的用户（在当前视频的弹幕发送者中）

    Args:
        target_uid: 推送对象（当前用户）的UID
        vid: 当前视频ID
        top_n: 返回前N个最相似的用户
        min_danmaku: 候选用户最少弹幕数

    Returns: [uid1, uid2, ...] 按相似度降序的用户ID列表
    """
    # 1. 获取目标用户的标签
    target_tags = get_user_tags(target_uid)
    if not target_tags:
        return []

    # 2. 获取当前视频的弹幕发送者（排除自己）
    candidate_uids = get_video_danmaku_users(vid, exclude_uid=target_uid, min_danmaku=min_danmaku)
    if not candidate_uids:
        return []

    # 3. 批量获取候选用户的标签
    conn = getDBConn()
    cursor = conn.cursor()

    placeholders = ','.join(['%s'] * len(candidate_uids))
    cursor.execute(f"""
        SELECT uid, tag_name, weight
        FROM user_tag
        WHERE uid IN ({placeholders})
    """, candidate_uids)

    candidate_tags = defaultdict(dict)
    for uid, tag_name, weight in cursor.fetchall():
        candidate_tags[uid][tag_name] = weight

    cursor.close()
    conn.close()

    # 4. 计算相似度并排序
    similarities = []
    for uid in candidate_uids:
        if uid not in candidate_tags:
            continue
        sim = cosine_similarity(target_tags, candidate_tags[uid])
        if sim > 0:
            similarities.append((uid, sim))

    # 5. 按相似度排序并取前N个
    similarities.sort(key=lambda x: x[1], reverse=True)
    return [uid for uid, _ in similarities[:top_n]]


def startSimilar(vid, target_uid, limit=5):
    """
    获取与目标用户最相似的弹幕发送者用户ID列表

    Args:
        vid: 视频ID
        target_uid: 当前用户ID
        limit: 返回数量限制

    Returns:
        [1001, 1002, 1003, ...]  # 按相似度降序的用户ID列表
    """
    return find_similar_users(target_uid, vid, top_n=limit, min_danmaku=1)


# ==================== 测试代码 ====================
if __name__ == "__main__":
    test_vid = 3
    test_uid = 123123123

    print(f"正在查找与用户 {test_uid} 相似的用户（视频 {test_vid}）...")
    similar_uids = startSimilar(test_vid, test_uid, limit=5)

    print(f"\n【相似用户ID列表】")
    print(similar_uids)

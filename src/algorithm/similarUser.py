# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : similarUser.py
# @Project : recommendation-algorithm

from src.util.database import connectMySql
import math
from collections import defaultdict
import heapq


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

    Args:
        vid: 视频ID
        exclude_uid: 需要排除的用户ID（通常是推送对象自己）
        min_danmaku: 最小弹幕数（过滤只发了一两条的用户）

    Returns: [uid1, uid2, ...]
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

    Args:
        tags1: {tag: weight, ...}
        tags2: {tag: weight, ...}

    Returns: 相似度 (0-1)
    """
    if not tags1 or not tags2:
        return 0

    # 找出共同的标签
    common_tags = set(tags1.keys()) & set(tags2.keys())
    if not common_tags:
        return 0

    # 计算点积
    dot_product = sum(tags1[tag] * tags2[tag] for tag in common_tags)

    # 计算模长
    norm1 = math.sqrt(sum(w ** 2 for w in tags1.values()))
    norm2 = math.sqrt(sum(w ** 2 for w in tags2.values()))

    if norm1 == 0 or norm2 == 0:
        return 0

    return dot_product / (norm1 * norm2)


def jaccard_similarity(tags1, tags2):
    """
    计算Jaccard相似度（只考虑标签是否存在，不考虑权重）

    Args:
        tags1: {tag: weight, ...}
        tags2: {tag: weight, ...}

    Returns: 相似度 (0-1)
    """
    set1 = set(tags1.keys())
    set2 = set(tags2.keys())

    if not set1 or not set2:
        return 0

    intersection = len(set1 & set2)
    union = len(set1 | set2)

    return intersection / union if union > 0 else 0


def weighted_jaccard_similarity(tags1, tags2):
    """
    计算加权Jaccard相似度（考虑权重）

    Args:
        tags1: {tag: weight, ...}
        tags2: {tag: weight, ...}

    Returns: 相似度 (0-1)
    """
    if not tags1 or not tags2:
        return 0

    all_tags = set(tags1.keys()) | set(tags2.keys())

    numerator = 0
    denominator = 0

    for tag in all_tags:
        w1 = tags1.get(tag, 0)
        w2 = tags2.get(tag, 0)
        numerator += min(w1, w2)
        denominator += max(w1, w2)

    return numerator / denominator if denominator > 0 else 0


def find_similar_users(target_uid, vid, top_n=10, min_danmaku=1, method='cosine'):
    """
    查找与目标用户最相似的用户（在当前视频的弹幕发送者中）

    Args:
        target_uid: 推送对象（当前用户）的UID
        vid: 当前视频ID
        top_n: 返回前N个最相似的用户
        min_danmaku: 候选用户最少弹幕数（过滤只发了一两条的用户）
        method: 相似度计算方法 ('cosine', 'jaccard', 'weighted_jaccard')

    Returns: [(uid, similarity), ...] 按相似度降序
    """
    # 1. 获取目标用户的标签
    target_tags = get_user_tags(target_uid)
    if not target_tags:
        print(f"用户 {target_uid} 没有标签数据")
        return []

    # 2. 获取当前视频的弹幕发送者（排除自己）
    candidate_uids = get_video_danmaku_users(vid, exclude_uid=target_uid, min_danmaku=min_danmaku)
    if not candidate_uids:
        print(f"视频 {vid} 没有其他弹幕发送者")
        return []

    # 3. 批量获取候选用户的标签
    conn = getDBConn()
    cursor = conn.cursor()

    # 用IN查询一次性获取所有候选用户的标签
    placeholders = ','.join(['%s'] * len(candidate_uids))
    cursor.execute(f"""
        SELECT uid, tag_name, weight
        FROM user_tag
        WHERE uid IN ({placeholders})
    """, candidate_uids)

    # 整理成 {uid: {tag: weight, ...}} 格式
    candidate_tags = defaultdict(dict)
    for uid, tag_name, weight in cursor.fetchall():
        candidate_tags[uid][tag_name] = weight

    cursor.close()
    conn.close()

    # 4. 计算相似度
    similarities = []

    for uid in candidate_uids:
        if uid not in candidate_tags:
            continue

        tags = candidate_tags[uid]

        if method == 'cosine':
            sim = cosine_similarity(target_tags, tags)
        elif method == 'jaccard':
            sim = jaccard_similarity(target_tags, tags)
        elif method == 'weighted_jaccard':
            sim = weighted_jaccard_similarity(target_tags, tags)
        else:
            sim = cosine_similarity(target_tags, tags)

        if sim > 0:  # 只保留有相似度的用户
            similarities.append((uid, sim))

    # 5. 排序取TopN
    similarities.sort(key=lambda x: x[1], reverse=True)

    return similarities[:top_n]


def get_similar_users_with_details(target_uid, vid, top_n=10):
    """
    获取相似用户及其详细信息（标签对比）
    """
    similar_users = find_similar_users(target_uid, vid, top_n, method='cosine')

    if not similar_users:
        return []

    # 获取目标用户的标签（用于对比显示）
    target_tags = get_user_tags(target_uid)

    result = []
    for uid, sim in similar_users:
        user_tags = get_user_tags(uid)

        # 找出共同标签
        common = set(target_tags.keys()) & set(user_tags.keys())
        common_tags = {tag: (target_tags[tag], user_tags[tag]) for tag in common}

        result.append({
            'uid': uid,
            'similarity': round(sim, 3),
            'common_tags': common_tags,
            'tag_count': len(user_tags)
        })

    return result


# ==================== API调用函数（供前端调用） ====================

def get_similar_users_for_video(vid, target_uid, limit=10):
    """
    供前端API调用的接口
    Args:
        vid: 视频ID
        target_uid: 当前用户ID
        limit: 返回数量限制
    Returns:
        {
            'target_uid': target_uid,
            'video_id': vid,
            'similar_users': [
                {
                    'uid': 123,
                    'similarity': 0.85,
                    'tags': ['二次元', '数码发烧友']  # 主要标签（权重最高的几个）
                },
                ...
            ]
        }
    """
    # 查找相似用户
    similar = find_similar_users(target_uid, vid, top_n=limit, method='cosine')

    result = {
        'target_uid': target_uid,
        'video_id': vid,
        'similar_users': []
    }

    for uid, sim in similar:
        # 获取该用户权重最高的3个标签用于展示
        tags = get_user_tags(uid)
        top_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)[:3]
        tag_names = [tag for tag, _ in top_tags]

        result['similar_users'].append({
            'uid': uid,
            'similarity': round(sim, 3),
            'tags': tag_names
        })

    return result


# ==================== 测试代码 ====================
if __name__ == "__main__":
    # 测试参数
    test_vid = 3  # 假设视频ID为1
    test_uid = 123123123  # 测试用户

    print(f"正在查找与用户 {test_uid} 相似的用户（视频 {test_vid}）...")

    # 方法1：获取相似用户列表
    similar = find_similar_users(test_uid, test_vid, top_n=5)
    print("\n【相似用户列表】")
    for uid, sim in similar:
        print(f"  用户 {uid}: 相似度 {sim:.3f}")

    # 方法2：获取详细信息
    print("\n【相似用户详细信息】")
    details = get_similar_users_with_details(test_uid, test_vid, top_n=3)
    for item in details:
        print(f"\n用户 {item['uid']} (相似度: {item['similarity']})")
        print(f"  共同标签: {item['common_tags']}")

    # 方法3：API格式（供前端调用）
    api_result = get_similar_users_for_video(test_vid, test_uid, limit=5)
    print(f"\n【API返回格式】")
    import json

    print(json.dumps(api_result, ensure_ascii=False, indent=2))
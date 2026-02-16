# _*_ coding : utf-8 _*_
# @Time : 2026/2/14
# @Author : Morton
# @File : semanticTag.py
# @Project : recommendation-algorithm

from src.util.word_segmentation import get_segmenter
from src.util.database import connectMySql
from collections import defaultdict
from src.util.jsonHandler import loadTags

# 初始化分词器
segmenter = get_segmenter(use_stopwords=True, use_pos_filter=True)

# 从JSON文件加载标签
SEMANTIC_TAGS = loadTags()

# 获取数据库连接
def getDBConn():
    return connectMySql()


# ==================== 工具函数 ====================

def load_video_tags():
    """
    加载所有已过审视频的标签
    返回: {vid: [tag1, tag2, ...]}
    """
    conn = getDBConn()
    cursor = conn.cursor()
    cursor.execute("SELECT vid, tags FROM video WHERE status = 1 AND tags IS NOT NULL")
    video_tags = {}
    for vid, tags_str in cursor.fetchall():
        if tags_str and tags_str.strip():
            # tags字段是用空格分隔的
            video_tags[vid] = tags_str.strip().split()
        else:
            video_tags[vid] = []
    cursor.close()
    conn.close()
    return video_tags


def match(text):
    """
    将一段文本匹配到语义标签（改进版：使用分词）
    返回: {tag_name: 匹配次数}
    """
    if not text:
        return {}
    # 分词并提取关键词
    words = segmenter.segment(text)
    keywords = segmenter.extract_keywords(text, topK=5)
    # 合并所有有效词
    effective_words = set(words + keywords)
    matches = defaultdict(int)
    text_lower = text.lower()
    for tag_name, keywords_list in SEMANTIC_TAGS.items():
        for keyword in keywords_list:
            # 检查关键词是否在有效词中，或者直接出现在原文中
            if keyword.lower() in effective_words or keyword.lower() in text_lower:
                matches[tag_name] += 1
    return dict(matches)


def calSemanticWeights(uid, video_tags_map, normalize=False):
    """
    计算指定用户的语义标签权重
    """
    conn = getDBConn()
    cursor = conn.cursor()
    # 初始化权重字典
    weights = {tag: 0 for tag in SEMANTIC_TAGS.keys()}
    # ===== 1. 从观看的视频标签中提取兴趣 =====
    cursor.execute("""
        SELECT vid, play, love, coin, collect
        FROM user_video
        WHERE uid = %s
    """, (uid,))
    user_videos = cursor.fetchall()
    for vid, play, love, coin, collect in user_videos:
        video_tags = video_tags_map.get(vid, [])
        if not video_tags:
            continue
        # 将视频的每个标签尝试匹配到语义标签
        video_text = " ".join(video_tags)  # 合并视频标签
        tag_matches = match(video_text)
        # 根据用户交互强度加权
        interaction_weight = 0
        if play > 0:
            interaction_weight += 1
        if love == 1:
            interaction_weight += 2
        if coin > 0:
            interaction_weight += coin
        if collect == 1:
            interaction_weight += 3
        for tag_name, match_count in tag_matches.items():
            # 视频标签每匹配一次，加上交互权重
            weights[tag_name] += interaction_weight * match_count
    # ===== 2. 从用户发送的弹幕中提取兴趣 =====
    cursor.execute("""
        SELECT content
        FROM danmu
        WHERE uid = %s AND status = 1
    """, (uid,))
    for (content,) in cursor.fetchall():
        tag_matches = match(content)
        for tag_name, match_count in tag_matches.items():
            # 每条弹幕匹配，权重+匹配次数
            weights[tag_name] += match_count
    cursor.close()
    conn.close()
    # 过滤掉权重为0的标签
    weights = {tag: w for tag, w in weights.items() if w > 0}
    if not weights:
        return {}

    if normalize:
        max_w = max(weights.values())
        if max_w > 0:
            return {tag: round(w / max_w, 4) for tag, w in weights.items()}
    return weights

# 综合优化
# def calSemanticWeights(uid, video_tags_map, normalize=False):
#     """
#     计算指定用户的语义标签权重（改进版）
#     """
#     conn = getDBConn()
#     cursor = conn.cursor()
#
#     # 初始化权重字典
#     weights = {tag: 0 for tag in SEMANTIC_TAGS.keys()}
#
#     # ===== 1. 从观看的视频标签中提取兴趣（权重系数0.6）=====
#     cursor.execute("""
#         SELECT vid, play, love, coin, collect, play_time
#         FROM user_video
#         WHERE uid = %s
#     """, (uid,))
#
#     user_videos = cursor.fetchall()
#     for vid, play, love, coin, collect, play_time in user_videos:
#         video_tags = video_tags_map.get(vid, [])
#         if not video_tags:
#             continue
#
#         # 将视频的每个标签尝试匹配到语义标签
#         video_text = " ".join(video_tags)
#         tag_matches = match(video_text)
#
#         # 根据用户交互强度加权
#         interaction_weight = 0
#         if play > 0:
#             interaction_weight += 1
#         if love == 1:
#             interaction_weight += 2
#         if coin > 0:
#             interaction_weight += coin
#         if collect == 1:
#             interaction_weight += 3
#
#         for tag_name, match_count in tag_matches.items():
#             weights[tag_name] += interaction_weight * match_count * 0.6  # 视频权重系数
#
#     # ===== 2. 从用户发送的弹幕中提取兴趣（权重系数1.0，带时间衰减）=====
#     cursor.execute("""
#         SELECT content, create_date
#         FROM danmu
#         WHERE uid = %s AND status = 1
#     """, (uid,))
#
#     now = datetime.datetime.now()
#     danmu_list = cursor.fetchall()
#
#     for (content, create_date) in danmu_list:
#         # 时间衰减
#         days_diff = (now - create_date).days
#         if days_diff <= 30:
#             time_factor = 1.0
#         elif days_diff <= 90:
#             time_factor = 0.8
#         elif days_diff <= 180:
#             time_factor = 0.5
#         else:
#             time_factor = 0.3
#
#         # 长度加权
#         length = len(content)
#         if length <= 5:
#             length_factor = 0.5
#         elif length <= 15:
#             length_factor = 1.0
#         else:
#             length_factor = 1.5
#
#         tag_matches = match(content)
#         for tag_name, match_count in tag_matches.items():
#             weights[tag_name] += match_count * time_factor * length_factor * 1.0  # 弹幕权重系数
#     cursor.close()
#     conn.close()
#     # 过滤掉权重为0的标签
#     weights = {tag: w for tag, w in weights.items() if w > 0}
#     if not weights:
#         return {}
#     if normalize:
#         max_w = max(weights.values())
#         if max_w > 0:
#             return {tag: round(w / max_w, 4) for tag, w in weights.items()}
#     return weights


def getOneUserSemanticTag(uid, normalize=False):
    """
    获取单个用户的语义标签权重
    """
    video_tags_map = load_video_tags()
    return calSemanticWeights(uid, video_tags_map, normalize)


def calAllUserSemantic(normalize=False):
    """
    计算所有正常用户的语义标签权重
    """
    video_tags_map = load_video_tags()
    conn = getDBConn()
    cursor = conn.cursor()
    cursor.execute("SELECT uid FROM user WHERE status = 0")
    users = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    all_tags = {}
    for uid in users:
        weights = calSemanticWeights(uid, video_tags_map, normalize)
        if weights:
            all_tags[uid] = weights
    return all_tags


def saveOneUserTags(uid, tags_dict):
    """
    将用户的语义标签保存到MySQL
    :param uid: 用户ID
    :param tags_dict: {tag_name: weight, ...}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 先删除该用户旧标签（或用INSERT ... ON DUPLICATE KEY UPDATE）
    cursor.execute("DELETE FROM user_tag WHERE uid = %s", (uid,))

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
    批量保存所有用户的语义标签
    :param data: {uid: {tag_name: weight, ...}}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 清空表（如果数据量不大，简单处理）
    cursor.execute("TRUNCATE TABLE user_tag")

    # 批量插入
    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
    values = []
    for uid, tags in data.items():
        for tag, weight in tags.items():
            values.append((uid, tag, weight))

    if values:
        # 分批插入，避免一次太多
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()

    cursor.close()
    conn.close()
    print(f"已保存 {len(values)} 条用户标签记录")

# ==================== 测试代码 ====================
if __name__ == "__main__":
    print("正在计算所有用户的语义标签...")
    user_tags = calAllUserSemantic(normalize=False)

    for i, (uid, tags) in enumerate(user_tags.items()):
        # if i >= 5:
        #     break
        # 按权重降序排序
        sorted_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)
        tag_display = [f"{tag}:{weight}" for tag, weight in sorted_tags]
        print(f"用户 {uid}: " + ", ".join(tag_display))

    # 测试单个用户
    # test_uid = 123123123
    # tags = getOneUserSemanticTag(test_uid)
    # print(f"\n用户 {test_uid} 的语义标签: {tags}")

    # 保存到数据库
    # print("正在保存到数据库...")
    # saveAllTags(user_tags)
    # print("完成！")

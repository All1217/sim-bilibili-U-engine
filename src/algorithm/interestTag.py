# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : interestTag.py (单用户精简版)
# @Project : recommendation-algorithm

from src.util.word_segmentation import get_segmenter
from src.util.database import connectMySql
from src.util.jsonHandler import loadTags
from collections import defaultdict
import datetime

# 初始化分词器
segmenter = get_segmenter(use_stopwords=True, use_pos_filter=True)

# 从JSON文件加载语义标签配置
SEMANTIC_TAGS = loadTags()

# 专业词库（用于弹幕质量，这里保留但不影响主逻辑）
PROFESSIONAL_WORDS = {
    "考研", "数学", "英语", "政治", "专业课", "高数", "线代", "概率论",
    "CPU", "GPU", "显卡", "内存", "硬盘", "SSD", "HDR", "4K", "8K",
    "算法", "数据结构", "编程", "代码", "调试", "框架", "数据库",
    "导数", "积分", "极限", "矩阵", "向量", "概率", "统计",
}


def getDBConn():
    """获取数据库连接"""
    return connectMySql()


def load_user_video_tags(uid):
    """
    加载用户观看过的所有视频的标签
    返回: {vid: [tag1, tag2, ...]}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 查询用户看过的所有视频及其标签
    cursor.execute("""
        SELECT v.vid, v.tags
        FROM video v
        JOIN user_video uv ON v.vid = uv.vid
        WHERE uv.uid = %s AND v.status = 1 AND v.tags IS NOT NULL
    """, (uid,))

    video_tags = {}
    for vid, tags_str in cursor.fetchall():
        if tags_str and tags_str.strip():
            video_tags[vid] = tags_str.strip().split()
        else:
            video_tags[vid] = []

    cursor.close()
    conn.close()
    return video_tags


def match_text_to_tags(text):
    """
    将文本匹配到语义标签
    返回: {tag_name: 匹配次数}
    """
    if not text:
        return {}

    # 分词并提取关键词
    words = segmenter.segment(text)
    keywords = segmenter.extract_keywords(text, topK=5)

    # 合并所有有效词
    effective_words = set(words + keywords)
    text_lower = text.lower()

    matches = defaultdict(int)
    for tag_name, keywords_list in SEMANTIC_TAGS.items():
        for keyword in keywords_list:
            if keyword.lower() in effective_words or keyword.lower() in text_lower:
                matches[tag_name] += 1

    return dict(matches)


def calculate_interest_tags(uid, use_time_decay=True, normalize=False):
    """
    计算单个用户的兴趣标签
    Args:
        uid: 用户ID
        use_time_decay: 是否使用时间衰减（推荐True）
        normalize: 是否归一化权重到[0,1]
    Returns:
        {tag_name: weight, ...}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 初始化权重字典
    weights = {tag: 0 for tag in SEMANTIC_TAGS.keys()}

    # ==================== 1. 从观看视频中提取兴趣 ====================
    cursor.execute("""
        SELECT v.vid, v.tags, uv.play, uv.love, uv.coin, uv.collect, uv.play_time
        FROM user_video uv
        JOIN video v ON uv.vid = v.vid
        WHERE uv.uid = %s AND v.status = 1
    """, (uid,))

    user_videos = cursor.fetchall()
    for vid, tags_str, play, love, coin, collect, play_time in user_videos:
        if not tags_str or not tags_str.strip():
            continue

        video_tags = tags_str.strip().split()
        video_text = " ".join(video_tags)
        tag_matches = match_text_to_tags(video_text)

        if not tag_matches:
            continue

        # 计算交互权重
        interaction_weight = 0
        if play > 0:
            interaction_weight += 1
        if love == 1:
            interaction_weight += 2
        if coin > 0:
            interaction_weight += coin
        if collect == 1:
            interaction_weight += 3

        # 视频源权重系数
        video_factor = 0.6

        # 时间衰减（如果启用）
        time_factor = 1.0
        if use_time_decay and play_time:
            days_diff = (datetime.datetime.now() - play_time).days
            if days_diff <= 30:
                time_factor = 1.0
            elif days_diff <= 90:
                time_factor = 0.8
            elif days_diff <= 180:
                time_factor = 0.6
            else:
                time_factor = 0.3

        for tag_name, match_count in tag_matches.items():
            weights[tag_name] += interaction_weight * match_count * video_factor * time_factor

    # ==================== 2. 从弹幕中提取兴趣 ====================
    cursor.execute("""
        SELECT content, create_date
        FROM danmu
        WHERE uid = %s AND status = 1
    """, (uid,))

    now = datetime.datetime.now()
    for content, create_date in cursor.fetchall():
        tag_matches = match_text_to_tags(content)

        if not tag_matches:
            continue

        # 时间衰减
        time_factor = 1.0
        if use_time_decay and create_date:
            days_diff = (now - create_date).days
            if days_diff <= 30:
                time_factor = 1.0
            elif days_diff <= 90:
                time_factor = 0.8
            elif days_diff <= 180:
                time_factor = 0.5
            else:
                time_factor = 0.2

        # 弹幕长度加权
        length = len(content)
        if length <= 5:
            length_factor = 0.5
        elif length <= 15:
            length_factor = 1.0
        else:
            length_factor = 1.5

        # 弹幕权重系数
        danmaku_factor = 1.0

        for tag_name, match_count in tag_matches.items():
            weights[tag_name] += match_count * time_factor * length_factor * danmaku_factor

    cursor.close()
    conn.close()
    # 过滤掉权重为0的标签
    result = {tag: w for tag, w in weights.items() if w > 0}
    if not result:
        return {}
    # 归一化
    if normalize:
        max_w = max(result.values())
        if max_w > 0:
            result = {tag: round(w / max_w, 4) for tag, w in result.items()}
    return result


def save_user_tags(uid, tags_dict):
    """
    将用户标签保存到数据库
    """
    if not tags_dict:
        print(f"用户 {uid} 没有标签数据，跳过保存")
        return

    conn = getDBConn()
    cursor = conn.cursor()

    # 删除该用户旧标签
    cursor.execute("DELETE FROM user_tag WHERE uid = %s", (uid,))

    # 批量插入新标签
    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
    values = [(uid, tag, weight) for tag, weight in tags_dict.items()]

    cursor.executemany(insert_sql, values)
    conn.commit()

    cursor.close()
    conn.close()
    print(f"用户 {uid} 的标签已保存，共 {len(values)} 条")


def build_user_interest_profile(uid, use_time_decay=True, normalize=False, auto_save=True):
    """
    构建用户兴趣画像（主函数）

    Args:
        uid: 用户ID
        use_time_decay: 是否使用时间衰减
        normalize: 是否归一化
        auto_save: 是否自动保存到数据库

    Returns:
        {tag_name: weight, ...}
    """
    print(f"开始构建用户 {uid} 的兴趣画像...")

    # 计算标签
    tags = calculate_interest_tags(uid, use_time_decay, normalize)

    if not tags:
        print(f"用户 {uid} 没有足够的兴趣数据")
        return {}

    # 打印结果
    sorted_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)
    tag_display = [f"{tag}:{weight}" for tag, weight in sorted_tags]
    print(f"用户 {uid} 的兴趣标签: {', '.join(tag_display)}")

    # 保存到数据库
    if auto_save:
        save_user_tags(uid, tags)

    return tags


# ==================== 测试代码 ====================
if __name__ == "__main__":
    import sys

    # 方式1：从命令行参数获取用户ID
    if len(sys.argv) > 1:
        test_uid = int(sys.argv[1])
    else:
        # 方式2：硬编码测试用户（可修改）
        test_uid = 123123123  # 替换为你要测试的用户ID

    print("=" * 60)
    print("🎯 单用户兴趣画像构建工具")
    print("=" * 60)

    # 构建用户画像（使用时间衰减，归一化，自动保存）
    tags = build_user_interest_profile(
        uid=test_uid,
        use_time_decay=True,
        normalize=False,  # 设为True可以归一化到0-1
        auto_save=True
    )

    if tags:
        print("\n✅ 画像构建完成")
    else:
        print("\n⚠️ 画像构建失败或数据不足")
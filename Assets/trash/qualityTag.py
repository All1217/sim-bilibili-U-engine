# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : qualityTag.py
# @Project : recommendation-algorithm

from src.util.database import connectMySql
from danmuScore import calDanmuScore, preprocess_danmaku_text
from src.util.word_segmentation import get_segmenter
from collections import defaultdict
import numpy as np

# 初始化分词器
segmenter = get_segmenter(use_stopwords=True)


def getDBConn():
    return connectMySql()


# ==================== 质量标签阈值配置 ====================
THRESHOLDS = {
    # 高质量弹幕：平均分 ≥ 0.7
    "high_quality_threshold": 0.7,
    # 低质量弹幕：平均分 ≤ 0.3
    "low_quality_threshold": 0.3,
    # 干货贡献者：专业词汇比例 ≥ 20%
    "professional_ratio_threshold": 0.2,
    # 玩梗达人：弹幕中梗词比例 ≥ 30%
    "meme_ratio_threshold": 0.3,
    # 稳定贡献者：至少发过20条弹幕
    "stable_contributor_min": 20,
    # 弹幕长度统计：长弹幕阈值（字符数）
    "long_danmaku_threshold": 20,
    # 短弹幕阈值
    "short_danmaku_threshold": 5
}

# ==================== 梗词库（可扩展） ====================
MEME_WORDS = {
    "2333", "hhhh", "草", "生草", "绝了", "离谱", "典", "典中典",
    "蚌埠住了", "绷不住了", "破防了", "我超", "卧槽", "我靠",
    "真香", "打脸", "翻车", "下车", "上车", "冲", "冲冲冲",
    "yyds", "YYDS", "永远的神", "绝绝子", "无语子", "笑死",
    "u1s1", "有一说一", "确实", "真实", "太真实了", "泪目",
    "名场面", "高能", "预警", "前方高能", "空降", "打卡"
}


# ==================== 核心计算函数 ====================

def load_all_danmakus_for_user(uid):
    """
    加载用户的所有弹幕
    返回: 弹幕对象列表，每条包含(text, create_date, vid)
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


def get_sender_profile(uid):
    """
    获取发送者的用户画像（从user_tag表）
    返回: 包含avg_like_rate等信息的字典
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 获取用户的所有标签
    cursor.execute("""
        SELECT tag_name, weight
        FROM user_tag
        WHERE uid = %s
    """, (uid,))

    profile = {}
    for tag_name, weight in cursor.fetchall():
        profile[tag_name] = weight

    # 计算活跃天数
    cursor.execute("""
        SELECT DATEDIFF(MAX(create_date), MIN(create_date)) as active_span
        FROM danmu
        WHERE uid = %s AND status = 1
    """, (uid,))

    active_span = cursor.fetchone()[0]
    if active_span:
        profile['active_days'] = active_span + 1  # +1是因为跨度从0开始算

    cursor.close()
    conn.close()

    return profile


def get_video_context_for_danmaku(vid, time_point):
    """
    获取弹幕所在的视频上下文（简化版）
    实际应用中可能需要更复杂的实现
    """

    # 这里简化处理，返回一个mock对象
    class VideoContext:
        def __init__(self):
            self.zone = None
            self.current_viewers = 100  # 默认值
            self.current_frame_keywords = []

        def count_similar(self, text):
            # 简化：假设没有相似弹幕
            return 0

    # 获取视频分区
    conn = getDBConn()
    cursor = conn.cursor()
    cursor.execute("SELECT mc_id FROM video WHERE vid = %s", (vid,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    context = VideoContext()
    if result:
        context.zone = result[0]

    return context


def calculate_danmaku_scores_for_user(uid):
    """
    计算用户所有弹幕的质量评分
    返回: 评分列表和详细统计
    """
    danmakus = load_all_danmakus_for_user(uid)
    if not danmakus:
        return [], {}

    sender_profile = get_sender_profile(uid)

    scores = []
    details = {
        'high_quality': 0,  # 高质量弹幕数
        'low_quality': 0,  # 低质量弹幕数
        'professional_count': 0,  # 含专业词汇的弹幕数
        'meme_count': 0,  # 含梗词的弹幕数
        'long_count': 0,  # 长弹幕数
        'short_count': 0,  # 短弹幕数
        'total_length': 0,  # 总字符数
        'scores': []  # 所有评分
    }

    for danmaku in danmakus:
        # 获取视频上下文
        context = get_video_context_for_danmaku(danmaku['vid'], danmaku['time_point'])

        # 计算评分
        score = calDanmuScore(danmaku, sender_profile, context)
        scores.append(score)
        details['scores'].append(score)

        # 统计各类别
        if score >= THRESHOLDS['high_quality_threshold']:
            details['high_quality'] += 1
        elif score <= THRESHOLDS['low_quality_threshold']:
            details['low_quality'] += 1

        # 预处理文本用于分析
        words = preprocess_danmaku_text(danmaku['text'])
        text = ' '.join(words)

        # 专业词汇判断（简化：使用PROFESSIONAL_WORDS，需要从danmuScore导入）
        from danmuScore import PROFESSIONAL_WORDS
        if any(word in PROFESSIONAL_WORDS for word in words):
            details['professional_count'] += 1

        # 梗词判断
        if any(meme in danmaku['text'] for meme in MEME_WORDS):
            details['meme_count'] += 1

        # 长度统计
        text_len = len(danmaku['text'])
        details['total_length'] += text_len
        if text_len >= THRESHOLDS['long_danmaku_threshold']:
            details['long_count'] += 1
        elif text_len <= THRESHOLDS['short_danmaku_threshold']:
            details['short_count'] += 1

    return scores, details


def getOneUserQualityTags(uid):
    """
    获取单个用户的质量维度标签
    返回: {tag_name: weight}  weight可以是数值或1表示有该标签
    """
    scores, details = calculate_danmaku_scores_for_user(uid)

    if not scores:
        return {}

    tags = {}
    total = len(scores)
    avg_score = np.mean(scores)

    # ===== 1. 总体质量标签 =====
    if avg_score >= THRESHOLDS['high_quality_threshold']:
        tags['高质量弹幕贡献者'] = round(avg_score, 2)
    elif avg_score <= THRESHOLDS['low_quality_threshold']:
        tags['低质量弹幕倾向'] = round(avg_score, 2)

    # ===== 2. 内容类型标签 =====
    professional_ratio = details['professional_count'] / total
    if professional_ratio >= THRESHOLDS['professional_ratio_threshold']:
        tags['干货贡献者'] = round(professional_ratio, 2)

    meme_ratio = details['meme_count'] / total
    if meme_ratio >= THRESHOLDS['meme_ratio_threshold']:
        tags['玩梗达人'] = round(meme_ratio, 2)

    # ===== 3. 形式特征标签 =====
    long_ratio = details['long_count'] / total
    short_ratio = details['short_count'] / total

    if long_ratio > 0.3 and long_ratio > short_ratio:
        tags['长文弹幕偏好'] = round(long_ratio, 2)
    elif short_ratio > 0.5:
        tags['短平快弹幕'] = round(short_ratio, 2)

    # ===== 4. 稳定性标签 =====
    if total >= THRESHOLDS['stable_contributor_min']:
        # 计算评分稳定性（标准差越小越稳定）
        std_dev = np.std(scores)
        stability = 1.0 - min(std_dev, 1.0)  # 简化：标准差越小，稳定性越高
        tags['稳定贡献者'] = round(stability, 2)

    # ===== 5. 极端值标签 =====
    if details['high_quality'] >= 5:  # 至少有5条高质量弹幕
        tags['精品弹幕制造机'] = details['high_quality']

    return tags


def calAllUserQuality():
    """
    计算所有正常用户的质量标签
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
        tags = getOneUserQualityTags(uid)
        if tags:
            all_tags[uid] = tags

    return all_tags


# ==================== 保存函数 ====================

def saveOneUserTags(uid, tags_dict):
    """
    将用户的质量标签保存到MySQL
    :param uid: 用户ID
    :param tags_dict: {tag_name: weight, ...}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 删除该用户的质量标签（避免重复）
    quality_tags = [
        "高质量弹幕贡献者", "低质量弹幕倾向", "干货贡献者", "玩梗达人",
        "长文弹幕偏好", "短平快弹幕", "稳定贡献者", "精品弹幕制造机"
    ]
    placeholders = ','.join(['%s'] * len(quality_tags))
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


def saveAllTags(data):
    """
    批量保存所有用户的质量标签
    :param data: {uid: {tag_name: weight, ...}}
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 删除所有质量标签
    quality_tags = [
        "高质量弹幕贡献者", "低质量弹幕倾向", "干货贡献者", "玩梗达人",
        "长文弹幕偏好", "短平快弹幕", "稳定贡献者", "精品弹幕制造机"
    ]
    placeholders = ','.join(['%s'] * len(quality_tags))
    cursor.execute(f"DELETE FROM user_tag WHERE tag_name IN ({placeholders})", quality_tags)

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
    print(f"已保存 {len(values)} 条用户质量标签记录")


# ==================== 分析辅助函数 ====================

def analyze_quality_distribution():
    """
    分析质量标签的分布情况
    """
    conn = getDBConn()
    cursor = conn.cursor()

    # 获取所有有弹幕的用户
    cursor.execute("""
        SELECT uid, COUNT(*) as cnt
        FROM danmu
        WHERE status = 1
        GROUP BY uid
        HAVING cnt >= 5
    """)

    users = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    if not users:
        print("暂无足够弹幕数据的用户")
        return

    print("=" * 60)
    print("📊 弹幕质量分布分析")
    print("=" * 60)

    all_avg_scores = []
    tag_stats = defaultdict(int)

    for uid in users[:100]:  # 抽样分析前100个用户
        tags = getOneUserQualityTags(uid)
        for tag in tags:
            tag_stats[tag] += 1

        # 计算平均分
        scores, _ = calculate_danmaku_scores_for_user(uid)
        if scores:
            all_avg_scores.append(np.mean(scores))

    print(f"\n【质量标签分布】（抽样 {len(users[:100])} 用户）")
    for tag, count in sorted(tag_stats.items(), key=lambda x: x[1], reverse=True):
        percentage = count / 100 * 100
        print(f"  {tag}: {count} 人 ({percentage:.1f}%)")

    if all_avg_scores:
        print(f"\n【整体质量统计】")
        print(f"  平均分均值: {np.mean(all_avg_scores):.2f}")
        print(f"  平均分中位数: {np.median(all_avg_scores):.2f}")
        print(f"  最高平均分: {max(all_avg_scores):.2f}")
        print(f"  最低平均分: {min(all_avg_scores):.2f}")

    print("=" * 60)


# ==================== 测试代码 ====================
if __name__ == "__main__":
    print("正在分析弹幕质量分布...")
    analyze_quality_distribution()

    print("\n正在计算所有用户的质量标签...")
    user_tags = calAllUserQuality()

    # 统计各标签数量
    tag_stats = {}
    for uid, tags in user_tags.items():
        for tag in tags:
            tag_stats[tag] = tag_stats.get(tag, 0) + 1

    print("\n各质量标签用户数：")
    for tag, count in sorted(tag_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"  {tag}: {count} 人")

    # 打印前10个用户的标签
    print("\n前10个用户的质量标签示例：")
    for i, (uid, tags) in enumerate(user_tags.items()):
        if i >= 10:
            break
        tag_list = [f"{tag}({weight})" for tag, weight in tags.items()]
        print(f"用户 {uid}: {', '.join(tag_list)}")

    # 保存到数据库（取消注释即可启用）
    print("\n正在保存到数据库...")
    saveAllTags(user_tags)
    print("完成！")
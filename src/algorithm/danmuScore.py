# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : danmuScore.py (移除点赞依赖版)
# @Project : recommendation-algorithm

from src.util.stopwords import get_stopwords_manager
from src.util.word_segmentation import get_segmenter

# 初始化分词器和停用词管理器
segmenter = get_segmenter(use_stopwords=True, use_pos_filter=False)
stopwords_mgr = get_stopwords_manager()

# 专业词库（可根据需要扩展）
PROFESSIONAL_WORDS = {
    "考研", "数学", "英语", "政治", "专业课", "高数", "线代", "概率论",
    "CPU", "GPU", "显卡", "内存", "硬盘", "SSD", "HDR", "4K", "8K",
    "算法", "数据结构", "编程", "代码", "调试", "框架", "数据库",
    "导数", "积分", "极限", "矩阵", "向量", "概率", "统计",
}


def preprocess_danmaku_text(text):
    """
    预处理弹幕文本：分词、去停用词、提取有效内容
    返回分词后的词列表
    """
    if not text:
        return []

    # 使用jieba分词并过滤停用词
    words = segmenter.segment(text)

    return words


def calculate_text_complexity(words):
    """
    计算文本复杂度
    Args:
        words: 分词后的词列表
    Returns:
        (长度得分, 专业度得分)
    """
    if not words:
        return 0, 0

    # 长度得分：词数越多越复杂（满分按10个词算）
    length_score = min(len(words) / 10, 1.0)

    # 专业度得分：包含专业词汇的比例
    professional_count = sum(1 for w in words if w in PROFESSIONAL_WORDS)
    if len(words) > 0:
        professional_ratio = professional_count / len(words)
        # 如果包含专业词汇，得分至少0.3
        technical_score = 0.3 + professional_ratio * 0.7
        technical_score = min(technical_score, 1.0)
    else:
        technical_score = 0.3

    return length_score, technical_score


def calDanmuScore(danmaku, sender_profile, video_context):
    """
    计算单条弹幕的质量评分

    Args:
        danmaku: 当前弹幕对象（可以是对象或字典），包含text字段
        sender_profile: 发送者的用户画像标签（包含active_days等）
        video_context: 视频上下文对象

    Returns:
        质量评分 (0-1)
    """
    score = 0

    # 兼容处理：无论是对象还是字典都能工作
    if isinstance(danmaku, dict):
        danmaku_text = danmaku.get('text', '')
    else:
        danmaku_text = getattr(danmaku, 'text', '')

    if not danmaku_text:
        return 0.0

    # 预处理弹幕文本，得到分词结果
    words = preprocess_danmaku_text(danmaku_text)
    clean_text = ' '.join(words)  # 用于相似度比较

    # ===== 1. 内容独特性（35%） =====
    # 使用分词后的文本计算相似度，避免停用词干扰
    uniqueness = 1.0 / (video_context.count_similar(clean_text) + 1)
    score += uniqueness * 0.35

    # ===== 2. 内容复杂度（35%） =====
    length_score, technical_score = calculate_text_complexity(words)
    complexity = length_score * 0.5 + technical_score * 0.5
    score += complexity * 0.35

    # ===== 3. 发送者经验（20%） =====
    # 基于用户的活跃天数评估（移除了点赞相关）
    authority = 0
    if 'active_days' in sender_profile:
        # 活跃天数越多，权威度越高（假设365天为满分）
        authority += min(sender_profile['active_days'] / 365, 1.0)
    else:
        authority += 0.5  # 默认中等权威度

    # 领域匹配度（如果用户在该领域有专家标签）
    if 'expert_in' in sender_profile and sender_profile['expert_in'] == video_context.zone:
        authority = min(authority + 0.2, 1.0)  # 专家加成

    score += authority * 0.20

    # ===== 4. 相关性（10%） =====
    # 使用分词后的关键词匹配视频当前画面
    keywords = segmenter.extract_keywords(danmaku_text, topK=3)
    if any(keyword in video_context.current_frame_keywords for keyword in keywords):
        score += 0.10

    return min(score, 1.0)


# ==================== 测试代码 ====================
if __name__ == "__main__":
    class TestDanmaku:
        def __init__(self, text):
            self.text = text


    class TestVideoContext:
        def __init__(self):
            self.zone = "科技"
            self.current_frame_keywords = ["电脑", "显卡"]

        def count_similar(self, text):
            # 模拟相似弹幕计数
            return 2


    test_cases = [
        "哈哈哈 这个显卡性能真强啊 2333",
        "考研数学太难了，求大神指教",
        "up主牛逼！三连了！",
        "这个镜头的景深控制得很好，光圈应该开到f/1.4了",
        "前方高能预警！"
    ]

    # 测试用用户画像（移除了点赞相关字段）
    test_profile = {
        'active_days': 200,  # 活跃200天
        'expert_in': '科技'  # 科技领域专家
    }
    test_context = TestVideoContext()

    print("=" * 60)
    print("🎯 弹幕质量评分测试（无点赞依赖版）")
    print("=" * 60)

    for text in test_cases:
        danmaku = TestDanmaku(text)
        score = calDanmuScore(danmaku, test_profile, test_context)

        # 显示预处理结果
        words = preprocess_danmaku_text(text)
        print(f"\n原文: {text}")
        print(f"分词: {'/'.join(words)}")
        print(f"评分: {score:.3f}")

    # 测试无专家标签的情况
    print("\n" + "=" * 60)
    print("测试普通用户（无专家标签）：")
    normal_profile = {
        'active_days': 30  # 只活跃30天
    }

    danmaku = TestDanmaku("这个算法实现得很巧妙")
    score = calDanmuScore(danmaku, normal_profile, test_context)
    words = preprocess_danmaku_text(danmaku.text)
    print(f"原文: {danmaku.text}")
    print(f"分词: {'/'.join(words)}")
    print(f"评分: {score:.3f}")
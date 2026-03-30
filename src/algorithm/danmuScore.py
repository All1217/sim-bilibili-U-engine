# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : danmuScore.py
# @Project : algorithm-engine

from src.util.wordHandler import get_segmenter
from src.util.jsonHandler import loadJson

PROFESSIONAL_WORDS = loadJson('professionalWords.json')

segmenter = get_segmenter(use_stopwords=True, use_pos_filter=False)


def preprocessText(text):
    """
    预处理弹幕文本：分词、去停用词、提取有效内容
    返回分词后的词列表
    """
    if not text:
        return []
    words = segmenter.segment(text)
    return words


def calComplexity(words):
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
    TODO: 完善算法内容
    Args:
        danmaku: 当前弹幕对象（可以是对象或字典），包含text字段
        sender_profile: 发送者的用户画像标签（包含active_days等）
        video_context: 视频上下文对象
    Returns:
        质量评分 (0-1)
    """
    score = 0
    if isinstance(danmaku, dict):
        danmaku_text = danmaku.get('text', '')
    else:
        danmaku_text = getattr(danmaku, 'text', '')
    if not danmaku_text:
        return 0.0
    # 预处理弹幕文本，得到分词结果
    words = preprocessText(danmaku_text)
    clean_text = ' '.join(words)  # 用于相似度比较
    # ===== 1. 内容独特性（35%） =====
    # 使用分词后的文本计算相似度，避免停用词干扰
    uniqueness = 1.0 / (video_context.count_similar(clean_text) + 1)
    score += uniqueness * 0.35
    # ===== 2. 内容复杂度（35%） =====
    length_score, technical_score = calComplexity(words)
    complexity = length_score * 0.5 + technical_score * 0.5
    score += complexity * 0.35
    # ===== 3. 发送者经验（20%） =====
    # 基于用户的活跃天数评估
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
    keywords = segmenter.extractKeywords(danmaku_text, topK=3)
    if any(keyword in video_context.current_frame_keywords for keyword in keywords):
        score += 0.10
    return min(score, 1.0)

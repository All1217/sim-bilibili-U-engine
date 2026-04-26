# _*_ coding : utf-8 _*_
# @Time   : 2026
# @Author : Morton
# @File   : uniqueness.py
# @Project: algorithm-engine
# @Desc   : 基于 TF-IDF + 余弦相似度的弹幕内容独特性计算模块

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def calculate_uniqueness_scores(danmaku_texts):
    """
    计算一批弹幕中每条弹幕的独特性得分

    算法原理：
      1. 使用 TF-IDF 将每条弹幕转换为高维向量
      2. 计算所有弹幕两两之间的余弦相似度矩阵
      3. 对每条弹幕，取与其他弹幕的最大相似度 θ_max
      4. 独特性得分 U = 1 / (θ_max + ε)，其中 ε = 1e-6
      5. 归一化到 [0, 1] 区间

    Args:
        danmaku_texts: list[str]
            弹幕文本列表，每条文本已做过分词和去停用词处理，
            词与词之间用空格分隔（即 preprocessText 后再 ' '.join(words) 的结果）

    Returns:
        list[float]: 每条弹幕的独特性得分，与输入顺序一一对应，取值范围 [0, 1]

    Notes:
        - 若输入为空列表，返回空列表
        - 若只有 1 条弹幕，独特性直接返回 [1.0]（无可比较对象，视为完全独特）
        - 依赖 scikit-learn，请确保已安装：pip install scikit-learn
    """
    n = len(danmaku_texts)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    # 1. 使用 TF-IDF 将文本转换为向量
    #    由于文本已经用 jieba 分过词且以空格分隔，需要指定 tokenizer 按空格切分
    vectorizer = TfidfVectorizer(
        tokenizer=lambda x: x.split(),
        lowercase=False      # 中文无需小写转换
    )
    tfidf_matrix = vectorizer.fit_transform(danmaku_texts)

    # 2. 计算所有弹幕两两之间的余弦相似度矩阵
    similarity_matrix = cosine_similarity(tfidf_matrix)

    # 3. 对每条弹幕，取与其他弹幕的最大相似度（排除自身，将对角线置 0）
    np.fill_diagonal(similarity_matrix, 0)
    max_similarities = np.max(similarity_matrix, axis=1)

    # 4. 计算独特性得分：U(d) = 1 / (θ_max + ε)
    epsilon = 1e-6
    uniqueness_scores = 1.0 / (max_similarities + epsilon)

    # 5. 归一化到 [0, 1] 区间（将最大值映射为 1）
    max_score = np.max(uniqueness_scores)
    if max_score > 0:
        uniqueness_scores = uniqueness_scores / max_score

    return uniqueness_scores.tolist()
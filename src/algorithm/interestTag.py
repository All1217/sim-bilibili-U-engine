# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : interestTag.py
# @Project : algorithm-engine
# TODO: batch_match_text_model不是真正意义上的异步批处理，而是每10条分块。引入官方批处理接口需要对现有系统架构做出较大规模修改，暂时不做

from src.util.wordHandler import get_segmenter
from src.util.database import mysql_cursor
from src.util.jsonHandler import loadJson
from collections import defaultdict
from typing import List, Dict
from src.algorithm.vectorization import getTagVectorManager
import datetime

# 初始化分词器
segmenter = get_segmenter(use_stopwords=True, use_pos_filter=True)

# 从JSON文件加载语义标签配置
SEMANTIC_TAGS = loadJson('tags.json')


def matchText(text):
    """
    将文本匹配到语义标签
    返回: {tag_name: 匹配次数}
    """
    if not text:
        return {}
    # 分词并提取关键词
    words = segmenter.segment(text)
    keywords = segmenter.extractKeywords(text, topK=5)
    # 合并所有有效词
    effective_words = set(words + keywords)
    text_lower = text.lower()
    matches = defaultdict(int)
    for tag_name, keywords_list in SEMANTIC_TAGS.items():
        for keyword in keywords_list:
            if keyword.lower() in effective_words or keyword.lower() in text_lower:
                matches[tag_name] += 1
    return dict(matches)


def batchMatchText(texts: List[str], use_vector: bool = True) -> List[Dict[str, int]]:
    """
    批量标签匹配函数
    """
    if not texts:
        return []
    results = [{} for _ in texts]
    if use_vector:
        try:
            manager = getTagVectorManager()
            # 找出需要向量处理的文本（非空）
            valid_indices = []
            valid_texts = []
            for i, text in enumerate(texts):
                if text and text.strip():
                    valid_indices.append(i)
                    valid_texts.append(text)
            if valid_texts:
                # 批量向量检索（现在内部会自动分块）
                batch_results = manager.batch_search_similar_tags(valid_texts, top_k=3)
                # 将结果放回原位
                success_count = 0
                for idx, matches in zip(valid_indices, batch_results):
                    if matches:  # 如果有匹配结果
                        converted = {}
                        for tag, similarity in matches.items():
                            if similarity > 0.5:
                                converted[tag] = max(1, min(10, int(similarity * 20)))
                        results[idx] = converted
                        success_count += 1
        except Exception as e:
            print(f"⚠️ 批量向量匹配失败: {e}，降级到关键词匹配")
            # 出错时降级，但只处理出错的这部分
            for i, text in enumerate(texts):
                if text and not results[i]:  # 只处理还没有结果的
                    try:
                        results[i] = matchText(text)
                    except:
                        pass
    # 如果 use_vector=False 或向量匹配完全失败，用关键词匹配补齐
    for i, text in enumerate(texts):
        if text and not results[i]:
            results[i] = matchText(text)
    return results


def calInterestTags(uid, use_time_decay=True, normalize=False):
    """
    计算单个用户的兴趣标签
    """
    # 初始化权重字典
    weights = {tag: 0 for tag in SEMANTIC_TAGS.keys()}
    # ==================== 1. 从观看视频中提取兴趣 ====================
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT v.vid, v.tags, uv.play, uv.love, uv.coin, uv.collect, uv.play_time
            FROM user_video uv
            JOIN video v ON uv.vid = v.vid
            WHERE uv.uid = %s AND v.status = 1
        """, (uid,))
        user_videos = cursor.fetchall()
    # 准备批量处理的视频文本
    video_texts = []
    video_metadata = []  # 存储对应的元数据
    for row in user_videos:
        tags_str = row['tags']
        if not tags_str or not tags_str.strip():
            continue
        video_text = " ".join(tags_str.strip().split())
        video_texts.append(video_text)
        video_metadata.append({
            'play': row['play'],
            'love': row['love'],
            'coin': row['coin'],
            'collect': row['collect'],
            'play_time': row['play_time']
        })
    # 批量匹配
    if video_texts:
        batch_matches = batchMatchText(video_texts)
        # 处理匹配结果
        for metadata, tag_matches in zip(video_metadata, batch_matches):
            if not tag_matches:
                continue
            # 计算交互权重
            interaction_weight = 0
            if metadata['play'] > 0:
                interaction_weight += 1
            if metadata['love'] == 1:
                interaction_weight += 2
            if metadata['coin'] > 0:
                interaction_weight += metadata['coin']
            if metadata['collect'] == 1:
                interaction_weight += 3
            # 视频源权重系数
            # TODO: 是否需要设置权重？有待商榷
            video_factor = 0.6
            # 时间衰减
            # TODO: Deepseek宣称这段时间衰减计算代码的依据是Ebbinghaus遗忘曲线理论
            # Ebbinghaus遗忘曲线（1885）：记忆随时间呈指数衰减，初期衰减快，后期趋缓。
            #
            time_factor = 1.0
            if use_time_decay and metadata['play_time']:
                days_diff = (datetime.datetime.now() - metadata['play_time']).days
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
    # ==================== 2. 从弹幕中提取兴趣（批量处理）====================
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT content, create_date
            FROM danmu
            WHERE uid = %s AND status = 1
        """, (uid,))
        danmaku_rows = cursor.fetchall()
    # 准备批量处理的弹幕文本
    danmaku_texts = []
    danmaku_dates = []
    for row in danmaku_rows:
        content = row['content']
        if content and content.strip():
            danmaku_texts.append(content)
            danmaku_dates.append(row['create_date'])
    now = datetime.datetime.now()
    # 批量匹配
    if danmaku_texts:
        batch_matches = batchMatchText(danmaku_texts)
        # 处理匹配结果
        for date, tag_matches in zip(danmaku_dates, batch_matches):
            if not tag_matches:
                continue
            # 时间衰减
            time_factor = 1.0
            if use_time_decay and date:
                days_diff = (now - date).days
                if days_diff <= 30:
                    time_factor = 1.0
                elif days_diff <= 90:
                    time_factor = 0.8
                elif days_diff <= 180:
                    time_factor = 0.5
                else:
                    time_factor = 0.2
            for tag_name, match_count in tag_matches.items():
                weights[tag_name] += match_count * time_factor
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


def saveTags(uid, tags_dict):
    """
    将用户标签保存到数据库
    """
    if not tags_dict:
        return
    try:
        with mysql_cursor() as cursor:
            cursor.execute("DELETE FROM user_tag WHERE uid = %s", (uid,))
            insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
            values = [(uid, tag, weight) for tag, weight in tags_dict.items()]
            cursor.executemany(insert_sql, values)
    except Exception as e:
        print(f"❌ 用户 {uid} 兴趣标签保存失败: {e}")
        raise


def geneInterestTags(uid, use_time_decay=True, normalize=False, auto_save=True):
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
    # 计算标签
    tags = calInterestTags(uid, use_time_decay, normalize)
    if not tags:
        return {}
    # 保存到数据库
    if auto_save:
        saveTags(uid, tags)
    return tags

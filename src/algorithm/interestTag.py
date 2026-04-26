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

segmenter = get_segmenter(use_stopwords=True, use_pos_filter=True)
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


def calInterestTags(uid, use_time_decay=True, normalize=False, preload=None):
    """
    计算单个用户的兴趣标签

    Args:
        uid: 用户ID
        use_time_decay: 是否使用时间衰减
        normalize: 是否归一化
        preload: 预加载数据 dict，由 profileBuilder 统一提供。
                 包含: 'user_video_rows', 'danmu_list'
                 若为 None 则自行查询数据库
    """
    weights = {tag: 0 for tag in SEMANTIC_TAGS.keys()}

    # ==================== 1. 从观看视频中提取兴趣 ====================
    if preload and 'user_video_rows' in preload:
        user_videos = preload['user_video_rows']
    else:
        with mysql_cursor() as cursor:
            cursor.execute("""
                SELECT v.vid, v.tags, uv.play, uv.love, uv.coin, uv.collect, uv.play_time
                FROM user_video uv
                JOIN video v ON uv.vid = v.vid
                WHERE uv.uid = %s AND v.status = 1
            """, (uid,))
            user_videos = cursor.fetchall()

    video_texts = []
    video_metadata = []
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

    if video_texts:
        batch_matches = batchMatchText(video_texts)
        for metadata, tag_matches in zip(video_metadata, batch_matches):
            if not tag_matches:
                continue
            interaction_weight = 0
            if metadata['play'] > 0:
                interaction_weight += 1
            if metadata['love'] == 1:
                interaction_weight += 2
            if metadata['coin'] > 0:
                interaction_weight += metadata['coin']
            if metadata['collect'] == 1:
                interaction_weight += 3
            video_factor = 0.6
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

    # ==================== 2. 从弹幕中提取兴趣 ====================
    if preload and 'danmu_list' in preload:
        danmaku_rows = preload['danmu_list']
        # 适配字段名：preload 中 key 为 'text' 而非 'content'
        danmaku_texts = []
        danmaku_dates = []
        for dm in danmaku_rows:
            text = dm.get('text', dm.get('content', ''))
            if text and text.strip():
                danmaku_texts.append(text)
                danmaku_dates.append(dm.get('create_date'))
    else:
        with mysql_cursor() as cursor:
            cursor.execute("""
                SELECT content, create_date
                FROM danmu
                WHERE uid = %s AND status = 1
            """, (uid,))
            rows = cursor.fetchall()
        danmaku_texts = []
        danmaku_dates = []
        for row in rows:
            if row['content'] and row['content'].strip():
                danmaku_texts.append(row['content'])
                danmaku_dates.append(row['create_date'])

    now = datetime.datetime.now()
    if danmaku_texts:
        batch_matches = batchMatchText(danmaku_texts)
        for date, tag_matches in zip(danmaku_dates, batch_matches):
            if not tag_matches:
                continue
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

    result = {tag: w for tag, w in weights.items() if w > 0}
    if not result:
        return {}

    if normalize:
        max_w = max(result.values())
        if max_w > 0:
            result = {tag: round(w / max_w, 4) for tag, w in result.items()}
    return result


def geneInterestTags(uid, use_time_decay=True, normalize=False, preload=None):
    """
    构建用户兴趣画像（主函数）
    """
    tags = calInterestTags(uid, use_time_decay, normalize, preload)
    return tags if tags else {}

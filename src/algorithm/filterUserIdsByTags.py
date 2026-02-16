# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : filterUserIdsByTags.py
# @Project : recommendation-algorithm

from src.util.database import connectMySql, connectRedis
import json
import redis
import hashlib
from typing import List, Set, Dict, Any

# 缓存过期时间（秒）
CACHE_EXPIRE = 300  # 5分钟


def getDBConn():
    return connectMySql()


def getRedisConn():
    return connectRedis()


def generate_cache_key(vid: int, user_ids: List[int], tags: List[str]) -> str:
    """
    生成缓存键
    格式: filter:vid:md5(用户ID列表+标签列表)
    """
    # 将列表排序后拼接，确保相同内容生成相同key
    user_str = ','.join(str(uid) for uid in sorted(set(user_ids)))
    tag_str = ','.join(sorted(tags))
    content = f"{vid}:{user_str}:{tag_str}"

    # 取MD5的前16位作为key，避免过长
    md5 = hashlib.md5(content.encode()).hexdigest()[:16]
    return f"filter:vid:{vid}:{md5}"


def get_users_with_tags(tag_list: List[str]) -> Set[int]:
    """
    获取拥有指定标签中任意一个的用户ID集合
    """
    if not tag_list:
        return set()

    conn = getDBConn()
    cursor = conn.cursor()

    # 构建IN查询
    placeholders = ','.join(['%s'] * len(tag_list))
    cursor.execute(f"""
        SELECT DISTINCT uid
        FROM user_tag
        WHERE tag_name IN ({placeholders})
    """, tag_list)

    users = {row[0] for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    return users


def filter_user_ids(
        vid: int,
        user_ids: List[int],
        tags: List[str],
        use_cache: bool = True
) -> List[int]:
    """
    过滤用户ID列表：只保留拥有指定标签的用户

    Args:
        vid: 视频ID（用于缓存）
        user_ids: 前端传入的用户ID列表（已去重或未去重）
        tags: 标签列表
        use_cache: 是否使用Redis缓存

    Returns:
        过滤后的用户ID列表（已去重）
    """
    if not user_ids or not tags:
        return []

    # 先去重
    unique_users = set(user_ids)

    # 尝试从缓存获取
    if use_cache:
        redis_client = getRedisConn()
        if redis_client:
            cache_key = generate_cache_key(vid, user_ids, tags)
            cached = redis_client.get(cache_key)
            if cached:
                print(f"缓存命中: {cache_key}")
                # 缓存中存的是JSON字符串
                return json.loads(cached)

    # 获取拥有这些标签的所有用户
    tagged_users = get_users_with_tags(tags)

    # 取交集：只保留那些既在传入列表里，又拥有标签的用户
    filtered = list(unique_users & tagged_users)

    # 存入缓存
    if use_cache and filtered:
        redis_client = getRedisConn()
        if redis_client:
            cache_key = generate_cache_key(vid, user_ids, tags)
            redis_client.setex(
                cache_key,
                CACHE_EXPIRE,
                json.dumps(filtered)
            )
            print(f"已缓存: {cache_key}")

    return filtered


def filter_with_stats(
        vid: int,
        user_ids: List[int],
        tags: List[str]
) -> Dict[str, Any]:
    """
    过滤并返回统计信息
    """
    original_count = len(set(user_ids))
    filtered = filter_user_ids(vid, user_ids, tags)

    return {
        'filtered_ids': filtered,
        'stats': {
            'original_count': original_count,
            'filtered_count': len(filtered),
            'removed_count': original_count - len(filtered),
            'tags_used': tags
        }
    }


def batch_filter_by_tags(
        vid: int,
        user_id_groups: Dict[str, List[int]],
        tags: List[str]
) -> Dict[str, List[int]]:
    """
    批量过滤多组用户ID（优化版：只查一次DB）

    Args:
        vid: 视频ID
        user_id_groups: {'group1': [1,2,3], 'group2': [4,5,6]}
        tags: 标签列表

    Returns:
        {'group1': [1,2], 'group2': [5]}
    """
    # 获取所有拥有标签的用户（一次DB查询）
    tagged_users = get_users_with_tags(tags)

    result = {}
    for group_name, ids in user_id_groups.items():
        unique_ids = set(ids)
        filtered = list(unique_ids & tagged_users)
        result[group_name] = filtered

    return result


# ==================== 供Java调用的接口 ====================

def process_filter_request(
        vid: int,
        user_ids_json: str,
        tags_json: str,
        use_cache: bool = True
) -> str:
    """
    处理Java后端的过滤请求

    Args:
        vid: 视频ID
        user_ids_json: JSON格式的用户ID列表，如 "[1,2,3,4,5]"
        tags_json: JSON格式的标签列表，如 '["高质量弹幕贡献者", "干货贡献者"]'
        use_cache: 是否使用缓存

    Returns:
        JSON格式的过滤结果
    """
    try:
        # 解析JSON
        user_ids = json.loads(user_ids_json)
        tags = json.loads(tags_json)

        # 过滤
        filtered = filter_user_ids(vid, user_ids, tags, use_cache)

        # 返回结果
        return json.dumps({
            'code': 200,
            'message': 'success',
            'data': filtered,
            'count': len(filtered)
        }, ensure_ascii=False)

    except json.JSONDecodeError as e:
        return json.dumps({
            'code': 400,
            'message': f'JSON解析失败: {str(e)}',
            'data': []
        })
    except Exception as e:
        return json.dumps({
            'code': 500,
            'message': f'服务器错误: {str(e)}',
            'data': []
        })


def clear_cache(vid: int = None, pattern: str = None) -> str:
    """
    清除缓存（可由Java定时调用或手动触发）

    Args:
        vid: 指定视频ID，清除该视频的所有缓存
        pattern: 自定义模式，如 "filter:vid:123:*"

    Returns:
        操作结果
    """
    redis_client = getRedisConn()
    if not redis_client:
        return json.dumps({'code': 500, 'message': 'Redis连接失败'})

    try:
        if vid:
            # 清除指定视频的所有缓存
            pattern = f"filter:vid:{vid}:*"

        if pattern:
            keys = redis_client.keys(pattern)
            if keys:
                count = redis_client.delete(*keys)
                return json.dumps({
                    'code': 200,
                    'message': f'已清除 {count} 个缓存',
                    'deleted': count
                })
            else:
                return json.dumps({'code': 200, 'message': '没有匹配的缓存'})
        else:
            # 清除所有filter缓存
            keys = redis_client.keys("filter:*")
            if keys:
                count = redis_client.delete(*keys)
                return json.dumps({
                    'code': 200,
                    'message': f'已清除所有 {count} 个缓存'
                })
            else:
                return json.dumps({'code': 200, 'message': '缓存为空'})

    except Exception as e:
        return json.dumps({
            'code': 500,
            'message': f'清除缓存失败: {str(e)}'
        })


# ==================== 测试代码 ====================
if __name__ == "__main__":
    # 模拟前端传入的数据
    test_vid = 3
    test_user_ids = [123123123, 363052223, 418293023, 418293023, 418293023, 123123123, 203482300, 203482300, 203482300,
                     203482300, 363052223, 363052223, 418293023, 579771616, 579771616, 579771616,
                     579771616, 579771616, 123123123, 772994005, 123123123, 123123123,
                     123123123, 772994005, 123123123, 816670585, 816670585, 941140327, 647898815, 123123123, 418293023,
                     816670585]
    test_tags = ["二次元", "数码发烧友"]

    print("=" * 60)
    print("🎯 用户ID过滤测试")
    print("=" * 60)

    print(f"\n【输入参数】")
    print(f"  视频ID: {test_vid}")
    print(f"  用户ID列表: {test_user_ids}")
    print(f"  标签列表: {test_tags}")

    # 1. 基础过滤
    filtered = filter_user_ids(test_vid, test_user_ids, test_tags)
    print(f"\n【过滤结果】")
    print(f"  原始用户数: {len(set(test_user_ids))}")
    print(f"  过滤后用户数: {len(filtered)}")
    print(f"  保留的用户: {filtered}")

    # 2. 带统计信息的过滤
    result = filter_with_stats(test_vid, test_user_ids, test_tags)
    print(f"\n【统计信息】")
    print(f"  原始用户数: {result['stats']['original_count']}")
    print(f"  过滤后用户数: {result['stats']['filtered_count']}")
    print(f"  移除用户数: {result['stats']['removed_count']}")
    print(f"  使用的标签: {result['stats']['tags_used']}")

    # 3. 模拟Java接口调用
    user_ids_json = json.dumps(test_user_ids)
    tags_json = json.dumps(test_tags)

    response = process_filter_request(test_vid, user_ids_json, tags_json)
    print(f"\n【Java接口返回】")
    print(json.dumps(json.loads(response), indent=2, ensure_ascii=False))

    # 4. 测试批量过滤
    groups = {
        'group1': [123123123, 203482300, 999999999],
        'group2': [363052223, 418293023, 888888888]
    }
    batch_result = batch_filter_by_tags(test_vid, groups, test_tags)
    print(f"\n【批量过滤结果】")
    for group, ids in batch_result.items():
        print(f"  {group}: {ids}")

    # 5. 测试缓存（第二次调用应该命中缓存）
    print(f"\n【测试缓存】")
    print("  第一次调用（已执行）")
    filtered2 = filter_user_ids(test_vid, test_user_ids, test_tags)
    print(f"  第二次调用: 获取到 {len(filtered2)} 个用户（应命中缓存）")

    # 6. 清除缓存测试
    print(f"\n【清除缓存】")
    clear_result = clear_cache(vid=test_vid)
    print(json.loads(clear_result))

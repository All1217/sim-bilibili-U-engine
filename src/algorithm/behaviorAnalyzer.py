# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : behaviorAnalyzer.py
# @Project : algorithm-engine


from src.util.database import mysql_cursor, get_redis_client
from src.util.jsonHandler import saveJson, loadJson
from src.common.redisConstants import BEHAVIOR_THRESHOLD_KEY
import numpy as np
from datetime import datetime


def analyze(isSave=True):
    """
    分析行为标签的分布情况，帮助调整阈值
    Args:
        isSave: 是否将结果保存到JSON文件
    Returns:
        包含分析结果的字典
    """
    result = {
        "analyze_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "danmaku_distribution": {},
        "interval_distribution": [],
        "night_owl_analysis": {}
    }
    # ==================== 1. 弹幕数量分布 ====================
    with mysql_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM danmu
            WHERE status = 1
            GROUP BY uid
        """)
        counts = [row['cnt'] for row in cursor.fetchall()]  # 使用字典游标，通过列名访问
        if not counts:
            return result
        # 基础统计
        result["danmaku_distribution"] = {
            "total_users": len(counts),
            "min": int(min(counts)),
            "max": int(max(counts)),
            "mean": round(float(np.mean(counts)), 2),
            "median": float(np.median(counts)),
            "percentile_90": round(float(np.percentile(counts, 90)), 1),
            "percentile_75": round(float(np.percentile(counts, 75)), 1),
            "percentile_25": round(float(np.percentile(counts, 25)), 1)
        }

        # ==================== 2. 区间分布 ====================
        bins = [0, 5, 20, 50, 100, 500, 1000, float('inf')]
        labels = ['0-5条', '6-20条', '21-50条', '51-100条', '101-500条', '501-1000条', '1000条以上']
        interval_data = []
        for i in range(len(bins) - 1):
            count = sum(1 for c in counts if bins[i] < c <= bins[i + 1])
            percentage = round(count / len(counts) * 100, 1)
            interval_data.append({
                "interval": labels[i],
                "user_count": count,
                "percentage": percentage
            })
        result["interval_distribution"] = interval_data

        # ==================== 3. 夜猫子分析 ====================
        cursor.execute("""
            SELECT 
                uid,
                COUNT(*) as total,
                SUM(HOUR(create_date) BETWEEN 0 AND 5) as night_count
            FROM danmu
            WHERE status = 1
            GROUP BY uid
            HAVING total >= 10
        """)
        night_users = 0
        total_users = 0
        night_ratios = []  # 收集所有夜猫子比例，用于分析
        for row in cursor.fetchall():
            total = row['total']
            night_count = row['night_count']
            total_users += 1
            ratio = night_count / total
            night_ratios.append(ratio)
            if ratio >= 0.3:
                night_users += 1
        if total_users > 0:
            result["night_owl_analysis"] = {
                "qualified_users": total_users,  # 弹幕数≥10条的用户
                "night_owl_users": night_users,
                "night_owl_percentage": round(night_users / total_users * 100, 1),
                "avg_night_ratio": round(float(np.mean(night_ratios)), 3) if night_ratios else 0,
                "median_night_ratio": round(float(np.median(night_ratios)), 3) if night_ratios else 0
            }
    # 自动提交和关闭由上下文管理器处理
    if isSave:
        saveJson("behaviorAnalysis.json", result)

    return result


def resultPrint(result):
    """打印分析结果"""
    print("=" * 60)
    print("📊 用户行为分布分析报告")
    print("=" * 60)

    print("\n【弹幕数量分布】")
    print(f"  用户总数: {result['danmaku_distribution']['total_users']} 人")
    print(f"  最小值: {result['danmaku_distribution']['min']} 条")
    print(f"  最大值: {result['danmaku_distribution']['max']} 条")
    print(f"  平均值: {result['danmaku_distribution']['mean']} 条")
    print(f"  中位数: {result['danmaku_distribution']['median']} 条")
    print(f"  75分位数: {result['danmaku_distribution']['percentile_75']} 条")
    print(f"  90分位数: {result['danmaku_distribution']['percentile_90']} 条")

    print("\n【弹幕数量区间分布】")
    for item in result["interval_distribution"]:
        print(f"  {item['interval']}: {item['user_count']} 人 ({item['percentage']}%)")

    if result["night_owl_analysis"]:
        print("\n【夜猫子分析】（弹幕数≥10条的用户）")
        print(f"  符合条件用户: {result['night_owl_analysis']['qualified_users']} 人")
        print(f"  夜猫子用户: {result['night_owl_analysis']['night_owl_users']} 人")
        print(f"  夜猫子占比: {result['night_owl_analysis']['night_owl_percentage']}%")
        print(f"  凌晨时段平均占比: {result['night_owl_analysis']['avg_night_ratio'] * 100:.1f}%")


def recommend():
    """
    根据分析结果给出阈值建议
    """
    res = None
    resDict = {}
    try:
        res = loadJson("behaviorAnalysis.json")
    except FileNotFoundError:
        print("读取结果文件发生错误！正在尝试重新分析……")
        res = analyze(True)
    if res is None:
        return
    danmaku_dist = res.get('danmaku_distribution', {})
    interval_dist = res.get('interval_distribution', [])
    night_analysis = res.get('night_owl_analysis', {})
    # 互动积极分子阈值（取75分位数）
    active_threshold = int(danmaku_dist.get('percentile_75', 50))
    resDict['active_threshold'] = active_threshold
    # 潜水观望者阈值（取第一个区间的上限）
    passive_threshold = 5
    if interval_dist and len(interval_dist) > 0:
        passive_percent = interval_dist[0]['percentage']
    resDict['passive_threshold'] = passive_percent
    # 夜猫子阈值
    if night_analysis:
        night_percent = night_analysis.get('night_owl_percentage', 0)
        resDict['night_ratio_threshold'] = night_percent
        resDict['night_min_samples'] = 10
    try:
        saveJson("recommend.json", resDict)
        redis_client = get_redis_client()
        redis_client.hset(BEHAVIOR_THRESHOLD_KEY, mapping=resDict)
    except FileNotFoundError:
        print("保存失败！")
    except Exception as e:
        print(f"保存到Redis失败: {e}")


if __name__ == "__main__":
    # 独立运行时进行分析并保存
    # result = analyze(isSave=True)

    # 打印分析结果
    # resultPrint(loadJson('behaviorAnalysis.json'))

    # 输出并保存阈值建议的代码片段
    recommend()

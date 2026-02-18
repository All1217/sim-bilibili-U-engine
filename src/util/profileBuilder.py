# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : profileBuilder.py
# @Project : recommendation-algorithm

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.algorithm.interestTag import geneInterestTags
from src.algorithm.behaviorTag import geneBehaviorTags
from src.algorithm.qualityTag import geneQualityTags
from src.util.database import mysql_cursor


class UserProfileBuilder:
    def __init__(self, max_workers=3):
        """
        初始化用户画像构建器
        Args:
            max_workers: 最大并发线程数
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def buildOneProfile(self, uid, save_to_db=True):
        """
        构建单个用户的完整画像
        Args:
            uid: 用户ID
            save_to_db: 是否保存到数据库
        Returns:
            {
                'uid': uid,
                'interest_tags': {...},
                'behavior_tags': {...},
                'quality_tags': {...},
                'success': bool,
                'execution_time': float
            }
        """
        start_time = time.time()
        # 提交三个任务到线程池
        future_interest = self.executor.submit(self.startInterestModule, uid, False)
        future_behavior = self.executor.submit(self.startBehaviorModule, uid, False)
        future_quality = self.executor.submit(self.startQualityModule, uid, False)
        results = {
            'uid': uid,
            'interest_tags': None,
            'behavior_tags': None,
            'quality_tags': None,
            'success': False,
            'execution_time': 0
        }
        # 等待所有任务完成
        try:
            results['interest_tags'] = future_interest.result(timeout=300)  # 5分钟超时
            results['behavior_tags'] = future_behavior.result(timeout=300)
            results['quality_tags'] = future_quality.result(timeout=300)
            # 检查是否有至少一个模块成功
            if any([results['interest_tags'], results['behavior_tags'], results['quality_tags']]):
                results['success'] = True
                # 统一保存到数据库
                if save_to_db:
                    self.saveToDB(uid, results)
        except Exception as e:
            print(f"❌ 用户 {uid} 画像构建失败: {e}")
            results['success'] = False
        results['execution_time'] = round(time.time() - start_time, 2)
        return results

    def saveToDB(self, uid, results):
        """
        统一保存所有标签到数据库（使用连接池）
        """
        # 收集所有标签
        all_tags = {}
        if results['interest_tags']:
            all_tags.update(results['interest_tags'])
        if results['behavior_tags']:
            all_tags.update(results['behavior_tags'])
        if results['quality_tags']:
            all_tags.update(results['quality_tags'])

        if not all_tags:
            return

        try:
            # 使用上下文管理器自动处理连接和事务
            with mysql_cursor() as cursor:
                # 1. 先删除该用户所有旧标签
                cursor.execute("DELETE FROM user_tag WHERE uid = %s", (uid,))
                # 2. 批量插入新标签
                if all_tags:
                    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
                    values = [(uid, tag, weight) for tag, weight in all_tags.items()]
                    cursor.executemany(insert_sql, values)
        except Exception as e:
            print(f"❌ 用户 {uid} 标签保存失败: {e}")
            # 异常时自动回滚
            raise

    def startInterestModule(self, uid, save_to_db):
        """运行兴趣标签模块"""
        try:
            tags = geneInterestTags(
                uid=uid,
                use_time_decay=True,
                normalize=False,
                auto_save=save_to_db  # 由外部参数控制
            )
            return tags
        except Exception as e:
            print(f"❌ [兴趣模块] 用户 {uid} 失败: {e}")
            return None

    def startBehaviorModule(self, uid, save_to_db):
        """运行行为属性模块"""
        try:
            tags = geneBehaviorTags(
                uid=uid,
                include_extended=True,
                auto_save=save_to_db  # 由外部参数控制
            )
            return tags
        except Exception as e:
            print(f"❌ [行为模块] 用户 {uid} 失败: {e}")
            return None

    def startQualityModule(self, uid, save_to_db):
        """运行弹幕质量模块"""
        try:
            tags = geneQualityTags(
                uid=uid,
                auto_save=save_to_db  # 由外部参数控制
            )
            return tags
        except Exception as e:
            print(f"❌ [质量模块] 用户 {uid} 失败: {e}")
            return None

    def batchBuildOneProfile(self, uid_list, save_to_db=True, max_workers=5):
        """
        批量构建多个用户的画像（并发执行多个用户）
        Args:
            uid_list: 用户ID列表
            save_to_db: 是否保存到数据库
            max_workers: 并发用户数
        Returns:
            {
                'total': 10,
                'success': 8,
                'failed': 2,
                'results': [...],
                'execution_time': 123.45
            }
        """
        start_time = time.time()
        results = []
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_uid = {
                executor.submit(self.buildOneProfile, uid, save_to_db): uid
                for uid in uid_list
            }
            for future in as_completed(future_to_uid):
                uid = future_to_uid[future]
                try:
                    result = future.result(timeout=600)  # 10分钟超时
                    results.append(result)
                    if result['success']:
                        success_count += 1
                except Exception as e:
                    print(f"❌ 用户 {uid} 处理失败: {e}")
                    results.append({
                        'uid': uid,
                        'success': False,
                        'error': str(e)
                    })
        execution_time = round(time.time() - start_time, 2)
        summary = {
            'total': len(uid_list),
            'success': success_count,
            'failed': len(uid_list) - success_count,
            'results': results,
            'execution_time': execution_time
        }
        return summary


# 全局单例
_profile_builder = None


def getInstance(max_workers=3):
    """获取画像构建器单例"""
    global _profile_builder
    if _profile_builder is None:
        _profile_builder = UserProfileBuilder(max_workers=max_workers)
    return _profile_builder


def buildOne(uid, save_to_db=True):
    """
    构建单个用户画像
    """
    builder = getInstance()
    return builder.buildOneProfile(uid, save_to_db)


def batchBuild(uids, save_to_db=True, max_workers=5):
    """
    批量构建用户画像
    """
    builder = getInstance()
    return builder.batchBuildOneProfile(uids, save_to_db, max_workers)


if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("🎯 用户画像构建工具")
    print("=" * 60)

    if len(sys.argv) > 1:
        uids = [int(uid) for uid in sys.argv[1].split(',')]
        print(f"批量构建用户: {uids}")
        result = batchBuild(uids, save_to_db=True, max_workers=3)
        print(f"完成: 成功 {result['success']}/{result['total']}, 耗时 {result['execution_time']}秒")
    else:
        test_uid = 123123123
        print(f"构建单个用户: {test_uid}")
        result = buildOne(test_uid, save_to_db=True)
        if result['success']:
            print(f"✅ 成功, 耗时 {result['execution_time']}秒")
            tag_counts = sum(1 for v in [result['interest_tags'], result['behavior_tags'], result['quality_tags']] if v)
            print(f"   生成标签模块数: {tag_counts}/3")
            # 显示各模块标签数量
            if result['interest_tags']:
                print(f"   兴趣标签: {len(result['interest_tags'])} 个")
            if result['behavior_tags']:
                print(f"   行为标签: {len(result['behavior_tags'])} 个")
            if result['quality_tags']:
                print(f"   质量标签: {len(result['quality_tags'])} 个")
        else:
            print(f"❌ 失败")
# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : profileBuilder.py (精简打印版)
# @Project : recommendation-algorithm

import threading
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.util.database import connectMySql
from src.algorithm.interestTag import build_user_interest_profile
from src.algorithm.behaviorTag import build_user_behavior_profile
from src.algorithm.qualityTag import build_user_quality_profile


class UserProfileBuilder:
    def __init__(self, max_workers=3):
        """
        初始化用户画像构建器
        Args:
            max_workers: 最大并发线程数（默认3，正好对应三个模块）
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def build_single_user_profile(self, uid, save_to_db=True):
        """
        构建单个用户的完整画像（多线程并发执行三个模块）
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
        future_interest = self.executor.submit(self._run_interest_module, uid, save_to_db)
        future_behavior = self.executor.submit(self._run_behavior_module, uid, save_to_db)
        future_quality = self.executor.submit(self._run_quality_module, uid, save_to_db)

        # 收集结果
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

        except Exception as e:
            print(f"❌ 用户 {uid} 画像构建失败: {e}")
            results['success'] = False

        results['execution_time'] = round(time.time() - start_time, 2)

        return results

    def _run_interest_module(self, uid, save_to_db):
        """运行兴趣标签模块"""
        try:
            tags = build_user_interest_profile(
                uid=uid,
                use_time_decay=True,
                normalize=False,
                auto_save=save_to_db
            )
            return tags
        except Exception as e:
            print(f"❌ [兴趣模块] 用户 {uid} 失败: {e}")
            return None

    def _run_behavior_module(self, uid, save_to_db):
        """运行行为属性模块"""
        try:
            tags = build_user_behavior_profile(
                uid=uid,
                include_extended=True,
                auto_save=save_to_db
            )
            return tags
        except Exception as e:
            print(f"❌ [行为模块] 用户 {uid} 失败: {e}")
            return None

    def _run_quality_module(self, uid, save_to_db):
        """运行弹幕质量模块"""
        try:
            tags = build_user_quality_profile(
                uid=uid,
                auto_save=save_to_db
            )
            return tags
        except Exception as e:
            print(f"❌ [质量模块] 用户 {uid} 失败: {e}")
            return None

    def batch_build_profiles(self, uid_list, save_to_db=True, max_workers=5):
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

        # 使用线程池并发处理多个用户
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_uid = {
                executor.submit(self.build_single_user_profile, uid, save_to_db): uid
                for uid in uid_list
            }

            # 收集结果
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


# 创建全局单例
_profile_builder = None


def get_profile_builder(max_workers=3):
    """获取画像构建器单例"""
    global _profile_builder
    if _profile_builder is None:
        _profile_builder = UserProfileBuilder(max_workers=max_workers)
    return _profile_builder


def build_user_profile(uid, save_to_db=True):
    """
    对外提供的简化接口：构建单个用户画像
    """
    builder = get_profile_builder()
    return builder.build_single_user_profile(uid, save_to_db)


def batch_build_profiles(uid_list, save_to_db=True, max_workers=5):
    """
    对外提供的简化接口：批量构建用户画像
    """
    builder = get_profile_builder()
    return builder.batch_build_profiles(uid_list, save_to_db, max_workers)


# ==================== 测试代码 ====================
if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("🎯 用户画像构建工具")
    print("=" * 60)

    if len(sys.argv) > 1:
        # 从命令行参数获取用户ID列表
        uids = [int(uid) for uid in sys.argv[1].split(',')]
        print(f"批量构建用户: {uids}")
        result = batch_build_profiles(uids, save_to_db=True, max_workers=3)
        print(f"完成: 成功 {result['success']}/{result['total']}, 耗时 {result['execution_time']}秒")
    else:
        # 测试单个用户
        test_uid = 123123123
        print(f"构建单个用户: {test_uid}")
        result = build_user_profile(test_uid, save_to_db=True)
        if result['success']:
            print(f"✅ 成功, 耗时 {result['execution_time']}秒")
            tag_counts = sum(1 for v in [result['interest_tags'], result['behavior_tags'], result['quality_tags']] if v)
            print(f"   生成标签模块数: {tag_counts}/3")
        else:
            print(f"❌ 失败")
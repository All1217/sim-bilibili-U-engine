# _*_ coding : utf-8 _*_
# @Time : 2026/2/18
# @Author : Morton
# @File : profileBuilder.py
# @Project : algorithm-engine

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.algorithm.interestTag import geneInterestTags
from src.algorithm.behaviorTag import geneBehaviorTags
from src.algorithm.qualityTag import geneQualityTags
from src.util.database import mysql_cursor


class UserProfileBuilder:
    def __init__(self, max_workers=3):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    # ==================== 数据预加载 ====================

    def _preloadUserData(self, uid):
        """
        一次性加载用户画像计算所需的所有表数据，存入内存。
        返回一个 dict，各模块可从中按需取用，避免重复查询数据库。

        预加载内容：
          - danmu_list:   用户所有弹幕 (id, content, create_date, vid, time_point)
          - danmu_count:  弹幕总数（直接 len）
          - danmu_hours:  每条弹幕的发送小时数列表
          - vid_mc_map:   弹幕涉及视频的 vid -> mc_id 映射
          - user_video_rows: 用户观看视频记录（含 video.tags）
        """
        preload = {}

        # 1. 加载用户所有弹幕（一次查询覆盖三个模块的全部 danmu 需求）
        with mysql_cursor() as cursor:
            cursor.execute("""
                SELECT id, content, create_date, vid, time_point
                FROM danmu
                WHERE uid = %s AND status = 1
                ORDER BY create_date
            """, (uid,))
            rows = cursor.fetchall()

        danmu_list = []
        danmu_hours = []
        vid_set = set()
        for row in rows:
            danmu_list.append({
                'id': row['id'],
                'text': row['content'],
                'create_date': row['create_date'],
                'vid': row['vid'],
                'time_point': row['time_point']
            })
            if row['create_date']:
                danmu_hours.append(row['create_date'].hour)
            vid_set.add(row['vid'])

        preload['danmu_list'] = danmu_list
        preload['danmu_count'] = len(danmu_list)
        preload['danmu_hours'] = danmu_hours

        # 2. 加载弹幕涉及视频的 mc_id（替代逐条查询 getVideoContext）
        if vid_set:
            with mysql_cursor() as cursor:
                placeholders = ','.join(['%s'] * len(vid_set))
                cursor.execute(
                    f"SELECT vid, mc_id FROM video WHERE vid IN ({placeholders})",
                    tuple(vid_set)
                )
                vid_rows = cursor.fetchall()
            preload['vid_mc_map'] = {row['vid']: row['mc_id'] for row in vid_rows}
        else:
            preload['vid_mc_map'] = {}

        # 3. 加载用户观看视频记录（供 interestTag 使用）
        with mysql_cursor() as cursor:
            cursor.execute("""
                SELECT v.vid, v.tags, uv.play, uv.love, uv.coin, uv.collect, uv.play_time
                FROM user_video uv
                JOIN video v ON uv.vid = v.vid
                WHERE uv.uid = %s AND v.status = 1
            """, (uid,))
            preload['user_video_rows'] = cursor.fetchall()

        return preload

    # ==================== 单个画像构建 ====================

    def buildOneProfile(self, uid, save_to_db=True):
        start_time = time.time()

        # 预加载：所有表数据只查一次
        try:
            preload = self._preloadUserData(uid)
        except Exception:
            print(f"❌ 用户 {uid} 数据预加载失败:")
            traceback.print_exc()
            return {
                'uid': uid,
                'interest_tags': None,
                'behavior_tags': None,
                'quality_tags': None,
                'success': False,
                'execution_time': round(time.time() - start_time, 2)
            }

        # 三个模块并发执行，传入预加载数据
        future_interest = self.executor.submit(
            self.startInterestModule, uid, preload
        )
        future_behavior = self.executor.submit(
            self.startBehaviorModule, uid, preload
        )
        future_quality = self.executor.submit(
            self.startQualityModule, uid, preload
        )

        results = {
            'uid': uid,
            'interest_tags': None,
            'behavior_tags': None,
            'quality_tags': None,
            'success': False,
            'execution_time': 0
        }

        try:
            results['interest_tags'] = future_interest.result(timeout=300)
            results['behavior_tags'] = future_behavior.result(timeout=300)
            results['quality_tags'] = future_quality.result(timeout=300)

            if any([results['interest_tags'], results['behavior_tags'], results['quality_tags']]):
                results['success'] = True
                if save_to_db:
                    self.saveToDB(uid, results)
        except Exception:
            print(f"❌ 用户 {uid} 画像构建失败:")
            traceback.print_exc()
            results['success'] = False

        results['execution_time'] = round(time.time() - start_time, 2)
        return results

    # ==================== 数据库保存 ====================

    def saveToDB(self, uid, results):
        all_tags = {}
        if results['interest_tags']:
            all_tags.update(results['interest_tags'])
        if results['behavior_tags']:
            all_tags.update(results['behavior_tags'])
        if results['quality_tags']:
            all_tags.update(results['quality_tags'])

        if not all_tags:
            return

        max_weight = max(all_tags.values())
        if max_weight > 0:
            normalized_tags = {
                tag: round(weight / max_weight, 4)
                for tag, weight in all_tags.items()
            }
        else:
            normalized_tags = all_tags

        try:
            with mysql_cursor() as cursor:
                cursor.execute("DELETE FROM user_tag WHERE uid = %s", (uid,))
                if normalized_tags:
                    insert_sql = "INSERT INTO user_tag (uid, tag_name, weight) VALUES (%s, %s, %s)"
                    values = [(uid, tag, weight) for tag, weight in normalized_tags.items()]
                    cursor.executemany(insert_sql, values)
        except Exception:
            print(f"❌ 用户 {uid} 标签保存失败:")
            traceback.print_exc()
            raise

    # ==================== 三个模块入口 ====================

    def startInterestModule(self, uid, preload):
        try:
            return geneInterestTags(uid=uid, use_time_decay=True, normalize=False, preload=preload)
        except Exception:
            print(f"❌ [兴趣模块] 用户 {uid} 失败:")
            traceback.print_exc()
            return None

    def startBehaviorModule(self, uid, preload):
        try:
            return geneBehaviorTags(uid=uid, include_extended=True, preload=preload)
        except Exception:
            print(f"❌ [行为模块] 用户 {uid} 失败:")
            traceback.print_exc()
            return None

    def startQualityModule(self, uid, preload):
        try:
            return geneQualityTags(uid=uid, preload=preload)
        except Exception:
            print(f"❌ [质量模块] 用户 {uid} 失败:")
            traceback.print_exc()
            return None

    # ==================== 批量构建 ====================

    def batchBuildOneProfile(self, uid_list, save_to_db=True, max_workers=5):
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
                    result = future.result(timeout=600)
                    results.append(result)
                    if result['success']:
                        success_count += 1
                except Exception:
                    print(f"❌ 用户 {uid} 处理失败:")
                    traceback.print_exc()
                    results.append({'uid': uid, 'success': False})

        return {
            'total': len(uid_list),
            'success': success_count,
            'failed': len(uid_list) - success_count,
            'results': results,
            'execution_time': round(time.time() - start_time, 2)
        }


# 全局单例
_profile_builder = None


def getInstance(max_workers=3):
    global _profile_builder
    if _profile_builder is None:
        _profile_builder = UserProfileBuilder(max_workers=max_workers)
    return _profile_builder


def buildOne(uid, save_to_db=True):
    return getInstance().buildOneProfile(uid, save_to_db)


def batchBuild(uids, save_to_db=True, max_workers=5):
    return getInstance().batchBuildOneProfile(uids, save_to_db, max_workers)
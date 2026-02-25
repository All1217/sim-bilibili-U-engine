# _*_ coding : utf-8 _*_
# @Time : 2026/2/13 17:52
# @Author : Morton
# @File : testScanDanmu
# @Project : algorithm-engine
# @Theme : 遍历包含弹幕数数据的文本文件，将得到的数据存入数据库

import re
import random
from datetime import datetime, timedelta
from src.util.database import connectMySql


# 获取数据库连接
def getConnection():
    return connectMySql()


# 生成最近三天内的随机时间
def get_random_recent_time():
    end = datetime.now()
    start = end - timedelta(days=3)
    random_seconds = random.randint(0, int((end - start).total_seconds()))
    return start + timedelta(seconds=random_seconds)


# 简化版：只插入，不返回数据
def insert_danmaku_from_file(file_path, vid, batch_size=500):
    """
    从弹幕文件批量插入数据
    :param file_path: 弹幕文件路径
    :param vid: 视频ID
    :param batch_size: 批量插入大小
    """
    conn = None
    cursor = None

    try:
        conn = getConnection()
        cursor = conn.cursor()

        with open(file_path, 'r', encoding='utf-8') as f:
            in_events = False
            batch_data = []

            for line in f:
                line = line.strip()

                if line == '[Events]':
                    in_events = True
                    continue

                if in_events and line.startswith('Dialogue:'):
                    # 提取时间点和文本
                    parts = line.split(',', 9)
                    if len(parts) >= 10:
                        start_time = parts[1].strip()
                        raw_text = parts[9].strip()

                        # 清洗文本
                        clean_text = re.sub(r'\{.*?\}', '', raw_text).strip()

                        if clean_text:
                            # 解析时间点
                            time_parts = start_time.split(':')
                            if len(time_parts) == 3:
                                time_point = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + float(time_parts[2])
                            elif len(time_parts) == 2:
                                time_point = int(time_parts[0]) * 60 + float(time_parts[1])
                            else:
                                time_point = float(start_time)

                            batch_data.append((
                                vid,
                                random.randint(1, 20),  # uid
                                clean_text,
                                25,  # fontsize
                                1,  # mode
                                '#FFFFFF',  # color
                                round(time_point, 2),
                                1,  # status
                                get_random_recent_time()
                            ))

                            # 批量插入
                            if len(batch_data) >= batch_size:
                                sql = """
                                    INSERT INTO danmu 
                                    (vid, uid, content, fontsize, mode, color, time_point, status, create_date) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                cursor.executemany(sql, batch_data)
                                conn.commit()
                                print(f"已插入 {len(batch_data)} 条弹幕...")
                                batch_data = []

            # 插入剩余数据
            if batch_data:
                sql = """
                    INSERT INTO danmu 
                    (vid, uid, content, fontsize, mode, color, time_point, status, create_date) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(sql, batch_data)
                conn.commit()
                print(f"✅ 成功插入最后 {len(batch_data)} 条弹幕")

            print(f"🎉 所有弹幕插入完成，视频ID: {vid}")

    except Exception as e:
        print(f"❌ 错误: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    file_path = 'C:\\Users\\Morton\\Desktop\\temp\\nanjing-danmu.txt'
    video_id = 35
    # 批量插入版
    insert_danmaku_from_file(file_path, video_id, batch_size=500)

# _*_ coding : utf-8 _*_
# @Time : 2024/12/11 15:35
# @Author : Morton
# @File : spider
# @Project : normal-utils

import pymysql
import random
from datetime import datetime, timedelta
from faker import Faker
from src.util.database import customizeMySqlConn
import time

# 初始化Faker，使用中文
fake = Faker('zh_CN')


def generate_consumption_data(num_records=5000, batch_size=1000):
    """
    生成消费记录测试数据
    :param num_records: 总记录数
    :param batch_size: 每批插入的数量
    """

    # 配置生成规则
    user_ids = list(range(1, 201))  # 假设有200个活跃用户，用户ID从1-200

    # 物品ID和分类的映射关系
    item_categories = {
        # 物品分类: 1-100 电子产品, 101-200 服装, 201-300 食品, 301-400 图书, 401-500 美妆
        (1, 100): 1,  # 电子产品
        (101, 200): 2,  # 服装
        (201, 300): 3,  # 食品
        (301, 400): 4,  # 图书
        (401, 500): 5,  # 美妆
        (501, 600): 6,  # 家居
        (601, 700): 7,  # 运动
        (701, 800): 8,  # 母婴
        (801, 900): 9,  # 宠物
        (901, 1000): 10,  # 其他
    }

    # 行为类型分布 (权重)
    behavior_types = [
        (1, 70),  # 浏览 - 70%
        (2, 15),  # 收藏 - 15%
        (3, 10),  # 加入购物车 - 10%
        (4, 5),  # 购买 - 5%
    ]

    # 时间范围：过去90天
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    def get_item_category(item_id):
        """根据物品ID获取分类"""
        for (start, end), category in item_categories.items():
            if start <= item_id <= end:
                return category
        return 0  # 未知分类

    def get_random_behavior():
        """按权重随机获取行为类型"""
        r = random.randint(1, 100)
        cumulative = 0
        for behavior, weight in behavior_types:
            cumulative += weight
            if r <= cumulative:
                return behavior
        return 1  # 默认返回浏览

    try:
        # 连接数据库
        conn = customizeMySqlConn('localhost', 'root', '123456', 'db01', True)
        cursor = conn.cursor()

        print(f"开始生成 {num_records} 条消费记录...")
        start_time = time.time()

        # 分批插入
        for batch_start in range(0, num_records, batch_size):
            batch_end = min(batch_start + batch_size, num_records)
            batch_records = []

            for i in range(batch_start, batch_end):
                # 随机选择用户
                user_id = random.choice(user_ids)

                # 随机生成物品ID (1-1000)
                item_id = random.randint(1, 1000)

                # 获取行为类型
                behavior = get_random_behavior()

                # 获取物品分类
                item_category = get_item_category(item_id)

                # 生成时间（越近的概率越大，使用指数分布）
                days_ago = int(random.expovariate(1 / 30))  # 平均30天前
                days_ago = min(days_ago, 90)  # 不超过90天
                record_time = end_date - timedelta(days=days_ago,
                                                   hours=random.randint(0, 23),
                                                   minutes=random.randint(0, 59),
                                                   seconds=random.randint(0, 59))

                batch_records.append((
                    user_id,
                    item_id,
                    behavior,
                    item_category,
                    record_time
                ))

            # 批量插入
            sql = """
                INSERT INTO comsuption 
                (user_id, item_id, behavior_type, item_category, time) 
                VALUES (%s, %s, %s, %s, %s)
            """

            cursor.executemany(sql, batch_records)
            conn.commit()

            print(f"已插入 {batch_end}/{num_records} 条记录...")

        end_time = time.time()
        print(f"✅ 成功插入 {num_records} 条数据！")
        print(f"⏱️  耗时: {end_time - start_time:.2f} 秒")

        # 显示数据分布统计
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(time) as earliest,
                MAX(time) as latest,
                AVG(behavior_type) as avg_behavior
            FROM comsuption
        """)
        stats = cursor.fetchone()
        print("\n📊 数据统计:")
        print(f"  总记录数: {stats[0]}")
        print(f"  独立用户数: {stats[1]}")
        print(f"  时间范围: {stats[2]} 到 {stats[3]}")

    except pymysql.Error as e:
        print(f"❌ 数据库错误: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        print(f"❌ 其他错误: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def generate_focused_data():
    """
    生成更有针对性的测试数据（适合特定场景）
    """
    try:
        conn = customizeMySqlConn('localhost', 'root', '123456', 'db01', True)
        cursor = conn.cursor()

        print("生成针对性测试数据...")

        # 场景1: 某用户的大量购买记录（测试用户画像）
        power_user_id = 1
        purchase_records = []
        for i in range(100):
            item_id = random.randint(1, 100)
            record_time = datetime.now() - timedelta(days=random.randint(0, 30))
            purchase_records.append((
                power_user_id,
                item_id,
                4,  # 购买行为
                item_id // 100 + 1,  # 分类
                record_time
            ))

        cursor.executemany(
            "INSERT INTO comsuption (user_id, item_id, behavior_type, item_category, time) VALUES (%s, %s, %s, %s, %s)",
            purchase_records
        )

        # 场景2: 热门物品的多行为记录（测试物品热度）
        hot_item_id = 888
        hot_item_records = []
        for i in range(200):
            user_id = random.randint(1, 50)
            behavior = random.choices([1, 2, 3, 4], weights=[50, 20, 20, 10])[0]
            record_time = datetime.now() - timedelta(days=random.randint(0, 7))
            hot_item_records.append((
                user_id,
                hot_item_id,
                behavior,
                8,  # 家居分类
                record_time
            ))

        cursor.executemany(
            "INSERT INTO comsuption (user_id, item_id, behavior_type, item_category, time) VALUES (%s, %s, %s, %s, %s)",
            hot_item_records
        )

        conn.commit()
        print("✅ 针对性测试数据生成完成！")

    except Exception as e:
        print(f"❌ 错误: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def geneUserData(num_records=5000, batch_size=1000):
    """
    生成用户画像测试数据
    :param num_records: 总记录数
    :param batch_size: 每批插入的数量
    """

    # 省份-城市映射（用于生成真实的省市关系）
    province_cities = {
        '北京市': ['北京市'],
        '上海市': ['上海市'],
        '广州市': ['广州市'],
        '深圳市': ['深圳市'],
        '浙江省': ['杭州市', '宁波市', '温州市', '嘉兴市'],
        '江苏省': ['南京市', '苏州市', '无锡市', '常州市'],
        '广东省': ['广州市', '深圳市', '珠海市', '佛山市'],
        '四川省': ['成都市', '绵阳市', '德阳市'],
        '湖北省': ['武汉市', '宜昌市', '襄阳市'],
        '陕西省': ['西安市', '咸阳市', '宝鸡市'],
        '山东省': ['济南市', '青岛市', '烟台市'],
        '辽宁省': ['沈阳市', '大连市'],
        '河南省': ['郑州市', '洛阳市'],
        '河北省': ['石家庄市', '保定市'],
        '湖南省': ['长沙市', '株洲市'],
        '福建省': ['福州市', '厦门市'],
        '安徽省': ['合肥市'],
        '重庆市': ['重庆市'],
        '天津市': ['天津市'],
    }
    # 职业列表（按不同教育层次分布）
    jobs_by_education = {
        '初中及以下': ['制造业工人', '建筑业工人', '餐饮服务员', '快递员', '保安', '保洁', '个体商户'],
        '高中/中专': ['销售员', '文员', '技术工人', '司机', '收银员', '客服专员'],
        '大专': ['行政专员', '销售经理', '技术员', '会计', '护士', '幼师', '电商运营'],
        '本科': ['软件工程师', '产品经理', '教师', '医生', '律师', '公务员', '设计师'],
        '硕士': ['高级工程师', '研究员', '大学教授', '主治医师', '高级经理'],
        '博士': ['科学家', '教授', '主任医师', '研究院院长', '技术总监']
    }
    # 教育层次分布
    education_levels = [
        ('初中及以下', 10),  # 10%
        ('高中/中专', 25),  # 25%
        ('大专', 30),  # 30%
        ('本科', 25),  # 25%
        ('硕士', 7),  # 7%
        ('博士', 3),  # 3%
    ]
    # 年龄分布（按城市等级）- 修复版本
    age_distribution = {
        '一线城市': {'weights': [10, 25, 35, 20, 10], 'range': [18, 22, 28, 35, 45, 60]},
        # 18-22, 23-28, 29-35, 36-45, 46-60
        '二线城市': {'weights': [15, 30, 30, 15, 10], 'range': [18, 22, 28, 35, 45, 55]},
        # 18-22, 23-28, 29-35, 36-45, 46-55
        '三线城市': {'weights': [20, 35, 25, 15, 5], 'range': [18, 22, 28, 35, 45, 50]},
        # 18-22, 23-28, 29-35, 36-45, 46-50
        '县城/乡镇': {'weights': [25, 35, 25, 10, 5], 'range': [18, 22, 28, 35, 45, 45]},
        # 18-22, 23-28, 29-35, 36-45, 46-45（实际就是46-45=无）
    }
    # 城市等级划分
    city_levels = {
        '一线城市': ['北京市', '上海市', '广州市', '深圳市'],
        '二线城市': ['杭州市', '南京市', '成都市', '武汉市', '西安市', '重庆市', '天津市', '苏州市'],
        '三线城市': ['宁波市', '无锡市', '珠海市', '合肥市', '长沙市', '青岛市', '沈阳市', '厦门市'],
        '县城/乡镇': ['嘉兴市', '常州市', '佛山市', '绵阳市', '宜昌市', '咸阳市', '烟台市', '大连市', '保定市',
                      '洛阳市'],
    }

    # 修复后的年龄生成函数
    def generate_age(city_level):
        """根据城市等级生成年龄"""
        dist = age_distribution[city_level]
        weights = dist['weights']
        ranges = dist['range']

        # 随机选择年龄段索引
        age_group_index = random.choices(range(len(weights)), weights=weights)[0]

        # 获取该年龄段的起始和结束年龄
        start_age = ranges[age_group_index]
        end_age = ranges[age_group_index + 1]

        # 在年龄段内随机生成年龄
        if start_age == end_age:
            return start_age
        return random.randint(start_age, end_age)

    # 婚姻状况与年龄关联
    def get_marriage_status(age):
        if age < 22:
            return '未婚'
        elif 22 <= age < 28:
            return random.choices(['未婚', '已婚'], weights=[60, 40])[0]
        elif 28 <= age < 35:
            return random.choices(['未婚', '已婚'], weights=[20, 80])[0]
        elif 35 <= age < 50:
            return random.choices(['已婚', '离异'], weights=[90, 10])[0]
        else:
            return random.choices(['已婚', '离异', '丧偶'], weights=[70, 20, 10])[0]

    # 性别分布（不同年龄段）
    def get_gender(age):
        if age < 30:
            return random.choices(['男', '女'], weights=[48, 52])[0]
        elif 30 <= age < 50:
            return random.choices(['男', '女'], weights=[51, 49])[0]
        else:
            return random.choices(['男', '女'], weights=[47, 53])[0]

    # 根据教育程度生成职业
    def get_job_by_education(edu):
        jobs = jobs_by_education.get(edu, ['其他'])
        return random.choice(jobs)

    # 根据年龄推荐教育程度
    def get_education_by_age(age):
        if age < 20:
            return '初中及以下'
        elif 20 <= age < 25:
            return random.choices(['大专', '本科'], weights=[40, 60])[0]
        elif 25 <= age < 35:
            edu = random.choices(
                ['大专', '本科', '硕士', '博士'],
                weights=[20, 55, 20, 5]
            )[0]
            return edu
        elif 35 <= age < 50:
            return random.choices(
                ['高中/中专', '大专', '本科', '硕士'],
                weights=[20, 35, 35, 10]
            )[0]
        else:
            return random.choices(
                ['初中及以下', '高中/中专', '大专', '本科'],
                weights=[15, 40, 30, 15]
            )[0]

    try:
        # 连接数据库
        conn = customizeMySqlConn('localhost', 'root', '123456', 'db01', True)
        cursor = conn.cursor()
        print(f"开始生成 {num_records} 条用户画像测试数据...")
        # 分批插入
        for batch_start in range(0, num_records, batch_size):
            batch_end = min(batch_start + batch_size, num_records)
            batch_records = []
            for i in range(batch_start, batch_end):
                # 1. 随机选择城市等级和城市
                city_level = random.choices(
                    list(city_levels.keys()),
                    weights=[15, 25, 30, 30]  # 一线15%，二线25%，三线30%，县城30%
                )[0]
                city = random.choice(city_levels[city_level])
                # 2. 根据城市等级确定省份
                province = None
                for prov, cities in province_cities.items():
                    if city in cities:
                        province = prov
                        break
                if not province:
                    province = random.choice(list(province_cities.keys()))
                # 3. 生成年龄（使用修复后的函数）
                age = generate_age(city_level)
                # 4. 生成性别
                gender = get_gender(age)
                # 5. 根据年龄生成教育程度
                education = get_education_by_age(age)
                # 6. 根据教育程度生成职业
                job = get_job_by_education(education)
                # 7. 根据年龄生成婚姻状况
                marriage = get_marriage_status(age)
                # 部分字段有概率为空
                if random.random() < 0.05:  # 5%的概率年龄为空
                    age = None
                if random.random() < 0.08:  # 8%的概率城市为空
                    city = None
                if random.random() < 0.08:  # 8%的概率省份为空
                    province = None
                if random.random() < 0.10:  # 10%的概率学历为空
                    education = None
                if random.random() < 0.10:  # 10%的概率职业为空
                    job = None
                batch_records.append((
                    age,
                    gender,
                    city,
                    province,
                    marriage,
                    education,
                    job
                ))
            # 批量插入
            sql = """
                INSERT INTO user 
                (age, gender, city, province, marriage, education, job) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, batch_records)
            conn.commit()
            print(f"已插入 {batch_end}/{num_records} 条记录...")
        print(f"✅ 成功插入 {num_records} 条用户画像数据！")
        # 显示数据分布统计
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                ROUND(AVG(age), 1) as avg_age,
                COUNT(DISTINCT city) as city_count,
                COUNT(DISTINCT province) as province_count,
                SUM(CASE WHEN gender = '男' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN gender = '女' THEN 1 ELSE 0 END) as female_count,
                SUM(CASE WHEN marriage = '已婚' THEN 1 ELSE 0 END) as married_count
            FROM user
        """)
        stats = cursor.fetchone()
        print("\n📊 数据统计:")
        print(f"  总记录数: {stats[0]}")
        print(f"  平均年龄: {stats[1] if stats[1] else 'N/A'}")
        print(f"  覆盖城市: {stats[2]}")
        print(f"  覆盖省份: {stats[3]}")
        print(f"  性别比例: 男 {stats[4]} / 女 {stats[5]}")
        print(f"  已婚人数: {stats[6]}")

    except pymysql.Error as e:
        print(f"❌ 数据库错误: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        print(f"❌ 其他错误: {e}")
        import traceback
        traceback.print_exc()  # 打印详细错误堆栈
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    # 生成用户数据
    geneUserData(10000)

    # 安装依赖提示
    # 生成消费数据
    # print("📦 请确保已安装依赖：pip install pymysql faker")
    # print("=" * 50)
    #
    # print("\n选择生成模式:")
    # print("1. 标准模式（5000条随机消费记录）")
    # print("2. 大数据模式（自定义数量）")
    # print("3. 针对性测试模式（特殊场景）")
    #
    # choice = input("请选择 (1/2/3): ")
    #
    # if choice == '1':
    #     generate_consumption_data(5000)
    # elif choice == '2':
    #     num = int(input("请输入要生成的记录数: "))
    #     generate_consumption_data(num)
    # elif choice == '3':
    #     generate_focused_data()
    #     generate_consumption_data(1000)  # 同时生成一些随机数据
    # else:
    #     print("无效选择，使用默认模式")
    #     generate_consumption_data(5000)

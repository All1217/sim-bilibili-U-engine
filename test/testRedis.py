import redis
import json
import src.config.application as config


class ConfigManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True  # 自动解码
        )
        self.config_key = "app:thresholds"  # Redis中的key

    def load_from_json(self, json_file_path):
        """从JSON文件加载配置到Redis"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            # 使用hset一次性存储所有配置
            self.redis_client.hset(self.config_key, mapping=config_data)
            print("配置已加载到Redis")
            return True
        except Exception as e:
            print(f"加载配置失败: {e}")
            return False

    def get_all_config(self):
        """获取所有配置"""
        config_data = self.redis_client.hgetall(self.config_key)
        if config_data:
            # 转换数值类型
            return {
                'active_threshold': float(config_data.get('active_threshold', 0)),
                'passive_threshold': float(config_data.get('passive_threshold', 0)),
                'night_ratio_threshold': float(config_data.get('night_ratio_threshold', 0)),
                'night_min_samples': int(config_data.get('night_min_samples', 0))
            }
        return None

    def get_config(self, field):
        """获取单个配置项"""
        value = self.redis_client.hget(self.config_key, field)
        if value:
            # 根据字段名返回合适的类型
            if field == 'night_min_samples':
                return int(value)
            else:
                return float(value)
        return None

    def update_config(self, field, value):
        """更新单个配置项"""
        self.redis_client.hset(self.config_key, field, value)
        print(f"配置已更新: {field} = {value}")

    def update_multiple_config(self, config_dict):
        """更新多个配置项"""
        self.redis_client.hset(self.config_key, mapping=config_dict)
        print(f"批量更新配置完成")

    def save_to_json(self, json_file_path):
        """将Redis中的配置保存到JSON文件"""
        config_data = self.get_all_config()
        if config_data:
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)
            print("配置已保存到JSON文件")
            return True
        return False


# 使用示例
if __name__ == "__main__":
    config_mgr = ConfigManager()

    # 1. 从JSON文件加载配置
    config_mgr.load_from_json("../Assets/recommend.json")

    # 2. 获取所有配置
    all_config = config_mgr.get_all_config()
    print(f"所有配置: {all_config}")

    # 3. 获取单个配置
    active = config_mgr.get_config("active_threshold")
    print(f"active_threshold: {active}")

    # 4. 更新配置
    config_mgr.update_config("active_threshold", 36.5)
    config_mgr.update_multiple_config({
        "passive_threshold": 20.5,
        "night_min_samples": 15
    })

    # 5. 保存回JSON文件
    config_mgr.save_to_json("../Assets/recommend.json")
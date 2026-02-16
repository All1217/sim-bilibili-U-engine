# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : stopwords.py (修复路径版)
# @Project : recommendation-algorithm

import json
import os

# 获取当前文件所在目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# 项目根目录（util -> src -> 根目录）
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
# Assets目录路径
ASSETS_DIR = os.path.join(PROJECT_ROOT, 'Assets')


class StopwordsManager:
    """停用词管理器"""

    def __init__(self, stopwords_file=None):
        """
        初始化停用词管理器

        Args:
            stopwords_file: 停用词文件路径。如果为None，则使用默认路径 Assets/stopwords.json
        """
        if stopwords_file is None:
            # 使用默认路径
            self.stopwords_file = os.path.join(ASSETS_DIR, 'stopwords.json')
        elif not os.path.isabs(stopwords_file):
            # 如果是相对路径，相对于Assets目录
            self.stopwords_file = os.path.join(ASSETS_DIR, stopwords_file)
        else:
            # 绝对路径直接使用
            self.stopwords_file = stopwords_file

        self.stopwords = self._load_stopwords()

    def _load_stopwords(self):
        """加载停用词文件"""
        if not os.path.exists(self.stopwords_file):
            print(f"⚠️ 停用词文件不存在: {self.stopwords_file}")
            # 尝试在Assets目录下查找
            alt_path = os.path.join(ASSETS_DIR, 'stopwords.json')
            if os.path.exists(alt_path):
                print(f"✅ 在Assets目录找到备用文件: {alt_path}")
                self.stopwords_file = alt_path
            else:
                print(f"❌ 停用词文件加载失败，返回空字典")
                return {}

        try:
            with open(self.stopwords_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ 加载停用词文件失败: {e}")
            return {}

    def get_all_stopwords(self):
        """获取所有停用词（返回集合，去重）"""
        all_words = set()
        for category, words in self.stopwords.items():
            if isinstance(words, list):
                all_words.update(words)
        return all_words

    def get_category(self, category):
        """获取特定类别的停用词"""
        return self.stopwords.get(category, [])

    def is_stopword(self, word):
        """判断是否为停用词"""
        all_stopwords = self.get_all_stopwords()
        return word in all_stopwords

    def filter_text(self, text, split=True):
        """
        过滤文本中的停用词
        Args:
            text: 输入文本
            split: 是否分词（按空格分割）
        Returns:
            过滤后的词列表 或 过滤后的文本
        """
        if not text:
            return [] if split else ""

        # 先按空格分词（如果原文本有空格）
        if split:
            words = text.split()
            filtered = [w for w in words if not self.is_stopword(w)]
            return filtered
        else:
            # 简单处理：移除停用词（更复杂的需要分词器）
            # 这里保持原样返回，由调用方决定如何处理
            return text

    def get_professional_words(self):
        """
        获取专业词汇（用于技术评分）
        这里假设非停用词都可能包含专业词汇，具体由业务逻辑判断
        """
        # 可以从特定类别或外部词库加载
        # 暂时返回空，由调用方自定义专业词库
        return set()

    def reload(self):
        """重新加载停用词文件（可用于热更新）"""
        self.stopwords = self._load_stopwords()
        print("✅ 停用词已重新加载")
        return self.stopwords


# 创建全局单例
_stopwords_manager = None


def get_stopwords_manager(stopwords_file=None):
    """获取停用词管理器单例"""
    global _stopwords_manager
    if _stopwords_manager is None:
        _stopwords_manager = StopwordsManager(stopwords_file)
    return _stopwords_manager


def reload_stopwords():
    """重新加载停用词（外部调用接口）"""
    manager = get_stopwords_manager()
    return manager.reload()


# ==================== 测试代码 ====================
if __name__ == "__main__":
    print(f"项目根目录: {PROJECT_ROOT}")
    print(f"Assets目录: {ASSETS_DIR}")

    sw = get_stopwords_manager()

    # 检查是否成功加载
    if sw.stopwords:
        print("\n=== 停用词统计 ===")
        for category, words in sw.stopwords.items():
            if isinstance(words, list):
                print(f"{category}: {len(words)} 个词")

        all_count = len(sw.get_all_stopwords())
        print(f"\n总计: {all_count} 个停用词（去重后）")

        # 测试过滤
        test_text = "哈哈哈 这个视频 太 好笑了 2333 up主 牛逼"
        filtered = sw.filter_text(test_text)
        print(f"\n过滤前: {test_text}")
        print(f"过滤后: {' '.join(filtered)}")

        # 测试单例
        sw2 = get_stopwords_manager()
        print(f"\n单例测试: 同一个对象? {sw is sw2}")
    else:
        print("❌ 停用词加载失败，请检查文件路径")
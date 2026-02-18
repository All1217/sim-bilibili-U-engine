# _*_ coding : utf-8 _*_
# @Time : 2026/2/16
# @Author : Morton
# @File : stopwords.py (精简打印版)
# @Project : recommendation-algorithm

import json
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
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
            self.stopwords_file = os.path.join(ASSETS_DIR, 'stopwords.json')
        elif not os.path.isabs(stopwords_file):
            self.stopwords_file = os.path.join(ASSETS_DIR, stopwords_file)
        else:
            self.stopwords_file = stopwords_file
        self.stopwords = self.loadStopwords()

    def loadStopwords(self):
        """加载停用词文件"""
        try:
            with open(self.stopwords_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ 加载停用词文件失败: {e}")
            return {}

    def loadWords(self):
        """获取所有停用词（返回集合，去重）"""
        all_words = set()
        for category, words in self.stopwords.items():
            if isinstance(words, list):
                all_words.update(words)
        return all_words

    def getCategory(self, category):
        """获取特定类别的停用词"""
        return self.stopwords.get(category, [])

    def isStopword(self, word):
        """判断是否为停用词"""
        all_stopwords = self.loadWords()
        return word in all_stopwords

    def filter(self, text, split=True):
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
            filtered = [w for w in words if not self.isStopword(w)]
            return filtered
        else:
            # 本项目暂时不需要用
            return text


# 创建全局单例
_stopwords_manager = None


def getStopwordsBuilder(stopwords_file=None):
    """获取停用词管理器单例"""
    global _stopwords_manager
    if _stopwords_manager is None:
        _stopwords_manager = StopwordsManager(stopwords_file)
    return _stopwords_manager


if __name__ == "__main__":
    pass
# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : wordHandler.py
# @Project : algorithm-engine

import jieba
import jieba.analyse

from src.util.jsonHandler import loadJson


class WordSegmenter:
    """分词工具类，封装jieba分词功能"""

    def __init__(self, use_stopwords=True, use_pos_filter=False):
        """
        初始化分词器
        Args:
            use_stopwords: 是否使用停用词过滤
            use_pos_filter: 是否使用词性过滤（只保留名词、动词等）
        """
        self.stopwords = list(self.loadStopwords()) if use_stopwords else None
        self.use_pos_filter = use_pos_filter

        # 加载自定义词典（暂时没用到）
        self._load_custom_dict()

        # 词性保留列表（当use_pos_filter=True时生效）
        self.keep_pos = {
            'n', 'nr', 'nr1', 'nr2', 'nrj', 'nrf', 'ns', 'nsf', 'nt', 'nz',  # 名词
            'v', 'vd', 'vg', 'vf', 'vx', 'vi', 'vl', 'vn',  # 动词
            'a', 'ad', 'an', 'ag', 'al',  # 形容词
            'i', 'l',  # 成语和习语
            'j',  # 简称
        }

    def loadStopwords(self):
        tWords = loadJson('stopwords.json')
        all_words = set()
        for category, wordList in tWords.items():
            if isinstance(wordList, list):
                all_words.update(wordList)
        return all_words

    def isStopword(self, word):
        return word in self.stopwords

    def _load_custom_dict(self):
        """加载自定义词典（例如弹幕特有词汇）"""
        # 可以添加弹幕特有词汇，防止被错误切分
        custom_words = [
            "前方高能", "空降成功", "空降失败", "野生字幕君",
            "2333", "hhhh", "QAQ", "OTL", "or2",
            "up主", "阿婆主", "三连", "投币", "点赞",
            "二次元", "鬼畜", "番剧", "新番", "旧番",
            # 可以继续添加
        ]
        for word in custom_words:
            jieba.add_word(word)

    def segment(self, text, cut_all=False):
        """
        对文本进行分词
        Args:
            text: 输入文本
            cut_all: 是否使用全模式（默认精确模式）
        Returns:
            如果 return_pos=False: 返回词列表
            如果 return_pos=True: 返回 (word, pos) 元组列表
        """
        if not text or not isinstance(text, str):
            return []
        # 去除多余空格
        text = text.strip()
        if not text:
            return []
        else:
            # 普通分词模式
            if cut_all:
                wordList = list(jieba.cut(text, cut_all=True))
            else:
                wordList = list(jieba.cut(text))
        # 停用词过滤
        if self.stopwords:
            wordList = [w for w in wordList if not self.isStopword(w)]
        return wordList

    def extractKeywords(self, text, topK=5, with_weight=False):
        """
        提取文本关键词（基于TF-IDF）
        Args:
            text: 输入文本
            topK: 返回前K个关键词
            with_weight: 是否返回权重
        Returns:
            关键词列表 或 (关键词, 权重) 列表
        """
        # 分词（中间包含停用词过滤过程）
        wordList = self.segment(text)
        text = ' '.join(wordList)
        if with_weight:
            return jieba.analyse.extract_tags(text, topK=topK, withWeight=True)
        else:
            return jieba.analyse.extract_tags(text, topK=topK)


# 创建全局单例
_segmenter = None


def get_segmenter(use_stopwords=True, use_pos_filter=False):
    """获取分词器单例"""
    global _segmenter
    if _segmenter is None:
        _segmenter = WordSegmenter(use_stopwords, use_pos_filter)
    return _segmenter


# 测试代码
if __name__ == "__main__":
    segmenter = get_segmenter()
    test_cases = [
        "前方高能预警！这个视频太精彩了2333",
        "up主牛逼，三连了！",
        "这个显卡的性能真的很强，想入手",
        "哈哈哈 笑死我了 这操作太秀了",
        "考研党路过，数学真的好难QAQ"
    ]
    print("=== 分词测试 ===")
    for text in test_cases:
        print(f"\n原文: {text}")
        # 普通分词
        words = segmenter.segment(text)
        print(f"分词: {'/'.join(words)}")
        # 关键词提取
        keywords = segmenter.extractKeywords(text, topK=3)
        print(f"关键词: {keywords}")

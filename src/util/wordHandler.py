# _*_ coding : utf-8 _*_
# @Time : 2026/2/15
# @Author : Morton
# @File : wordHandler.py
# @Project : recommendation-algorithm

import jieba
import jieba.posseg as pseg
from src.util.stopwords import get_stopwords_manager


class WordSegmenter:
    """分词工具类，封装jieba分词功能"""

    def __init__(self, use_stopwords=True, use_pos_filter=False):
        """
        初始化分词器
        Args:
            use_stopwords: 是否使用停用词过滤
            use_pos_filter: 是否使用词性过滤（只保留名词、动词等）
        """
        self.stopwords_mgr = get_stopwords_manager() if use_stopwords else None
        self.use_pos_filter = use_pos_filter

        # 加载自定义词典（如果有）
        self._load_custom_dict()

        # 词性保留列表（当use_pos_filter=True时生效）
        self.keep_pos = {
            'n', 'nr', 'nr1', 'nr2', 'nrj', 'nrf', 'ns', 'nsf', 'nt', 'nz',  # 名词
            'v', 'vd', 'vg', 'vf', 'vx', 'vi', 'vl', 'vn',  # 动词
            'a', 'ad', 'an', 'ag', 'al',  # 形容词
            'i', 'l',  # 成语和习语
            'j',  # 简称
        }

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

    def segment(self, text, cut_all=False, return_pos=False):
        """
        对文本进行分词

        Args:
            text: 输入文本
            cut_all: 是否使用全模式（默认精确模式）
            return_pos: 是否返回词性标注

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

        if return_pos:
            # 词性标注模式
            words = list(pseg.cut(text))
        else:
            # 普通分词模式
            if cut_all:
                words = list(jieba.cut(text, cut_all=True))
            else:
                words = list(jieba.cut(text))

        # 停用词过滤
        if self.stopwords_mgr:
            if return_pos:
                words = [(w, pos) for w, pos in words
                         if not self.stopwords_mgr.is_stopword(w)]
            else:
                words = [w for w in words
                         if not self.stopwords_mgr.is_stopword(w)]

        # 词性过滤
        if self.use_pos_filter and return_pos:
            words = [(w, pos) for w, pos in words if pos in self.keep_pos]

        return words

    def extract_keywords(self, text, topK=5, with_weight=False):
        """
        提取文本关键词（基于TF-IDF）

        Args:
            text: 输入文本
            topK: 返回前K个关键词
            with_weight: 是否返回权重

        Returns:
            关键词列表 或 (关键词, 权重) 列表
        """
        import jieba.analyse

        # 先过滤停用词
        if self.stopwords_mgr:
            # jieba自带停用词功能，但我们可以用自定义的
            words = self.segment(text)
            text = ' '.join(words)

        if with_weight:
            return jieba.analyse.extract_tags(text, topK=topK, withWeight=True)
        else:
            return jieba.analyse.extract_tags(text, topK=topK)

    def get_noun_phrases(self, text):
        """
        提取名词短语（基于词性标注）

        Args:
            text: 输入文本

        Returns:
            名词短语列表
        """
        words = self.segment(text, return_pos=True)

        noun_phrases = []
        current_phrase = []

        for word, pos in words:
            # 如果是名词或形容词，加入到当前短语
            if pos in self.keep_pos:
                current_phrase.append(word)
            else:
                # 遇到非名词/形容词，结束当前短语
                if current_phrase:
                    noun_phrases.append(''.join(current_phrase))
                    current_phrase = []

        # 处理最后一个短语
        if current_phrase:
            noun_phrases.append(''.join(current_phrase))

        return noun_phrases


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

        # 带词性标注
        words_pos = segmenter.segment(text, return_pos=True)
        print(f"词性: {[(w, pos) for w, pos in words_pos]}")

        # 关键词提取
        keywords = segmenter.extract_keywords(text, topK=3)
        print(f"关键词: {keywords}")

        # 名词短语
        phrases = segmenter.get_noun_phrases(text)
        print(f"名词短语: {phrases}")
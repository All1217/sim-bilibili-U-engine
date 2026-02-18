# _*_ coding : utf-8 _*_
# @Time : 2026/2/18 17:07
# @Author : Morton
# @File : qualityAnalyzer
# @Project : algorithm-engine
# TODO: 完善弹幕质量相关阈值

from src.util.jsonHandler import saveJson


def startAnalyze():
    """
    暂时先返回默认值
    """
    default = {
        # 高质量弹幕：平均分 ≥ 0.7
        "high_quality_threshold": 0.7,
        # 低质量弹幕：平均分 ≤ 0.3
        "low_quality_threshold": 0.3,
        # 干货贡献者：专业词汇比例 ≥ 20%
        "professional_ratio_threshold": 0.2,
        # 稳定贡献者：至少发过20条弹幕
        "stable_contributor_min": 20,
        # 弹幕长度统计：长弹幕阈值（字符数）
        "long_danmaku_threshold": 20,
        # 短弹幕阈值
        "short_danmaku_threshold": 5
    }
    saveJson('qualityThreshold.json', default)


if __name__ == '__main__':
    startAnalyze()

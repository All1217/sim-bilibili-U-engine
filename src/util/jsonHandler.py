# _*_ coding : utf-8 _*_
# @Time : 2026/2/14 19:24
# @Author : Morton
# @File : jsonHandler
# @Project : algorithm-engine

import json
import os

# 获取当前文件所在目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# 项目根目录（向上两级：util -> src -> 根目录）
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
# Assets目录路径
ASSETS_DIR = os.path.join(PROJECT_ROOT, 'Assets')


# 获取Assets目录
def getAssetsPath(filename):
    return os.path.join(ASSETS_DIR, filename)


def loadJson(filename):
    path = getAssetsPath(filename)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def saveJson(filename, data):
    path = getAssetsPath(filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

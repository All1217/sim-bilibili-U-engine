# _*_ coding : utf-8 _*_
# @Time : 2026/2/14 19:24
# @Author : Morton
# @File : jsonHandler
# @Project : recommendation-algorithm

import json
import os

# 获取当前文件所在目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# 项目根目录（向上两级：util -> src -> 根目录）
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
# Assets目录路径
ASSETS_DIR = os.path.join(PROJECT_ROOT, 'Assets')

def get_asset_path(filename):
    """获取Assets目录下的文件路径"""
    return os.path.join(ASSETS_DIR, filename)

# 加载用户画像标签
def loadTags():
    file_path = get_asset_path('tags.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# 覆盖保存用户画像标签
def saveTags(tags_dict):
    file_path = get_asset_path('tags.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(tags_dict, f, ensure_ascii=False, indent=2)

# 带参数加载json文件
def loadJson(path):
    # 如果传入的是相对路径，尝试从Assets目录加载
    if not os.path.isabs(path):
        path = get_asset_path(path)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

# 带参数保存json文件
def saveJson(path, data):
    if not os.path.isabs(path):
        path = get_asset_path(path)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    tags = loadTags()
    print(tags)
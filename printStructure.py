# _*_ coding : utf-8 _*_
# @Time : 2026/2/15 21:27
# @Author : Morton
# @File : printStructure.py
# @Project : algorithm-engine

import os
import sys
from pathlib import Path


def print_project_structure(start_path='.', indent='', output_file=None, exclude_dirs=None):
    """
    打印项目结构（放在项目根目录运行）
    这个脚本具有通用性，放在任何文件夹里都能帮你梳理项目结构
    结果输出在本目录名为“project_structure.txt”的文件里
    :param start_path: 起始路径
    :param indent: 缩进
    :param output_file: 输出文件
    :param exclude_dirs: 排除的目录列表
    """
    if exclude_dirs is None:
        exclude_dirs = ['.git', '__pycache__', 'venv', 'env', '.idea', 'dist', 'build']

    try:
        items = sorted(os.listdir(start_path))
    except PermissionError:
        return

    for item in items:
        item_path = os.path.join(start_path, item)

        # 跳过排除的目录
        if os.path.isdir(item_path) and item in exclude_dirs:
            continue

        # 判断是文件还是目录
        if os.path.isdir(item_path):
            print(f"{indent}📁 {item}/")
            if output_file:
                output_file.write(f"{indent}📁 {item}/\n")
            # 递归打印子目录
            print_project_structure(item_path, indent + '    ', output_file, exclude_dirs)
        else:
            # 获取文件大小
            size = os.path.getsize(item_path)
            size_str = format_size(size)
            print(f"{indent}📄 {item} ({size_str})")
            if output_file:
                output_file.write(f"{indent}📄 {item} ({size_str})\n")


def format_size(size):
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


def save_to_file(content, filename='project_structure.txt'):
    """保存到文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"\n项目结构已保存到: {filename}")


def get_tree_command():
    """使用系统 tree 命令（如果可用）"""
    try:
        import subprocess
        result = subprocess.run(['tree', '-L', '3', '-I', '__pycache__|*.pyc|.git|venv'],
                                capture_output=True, text=True, cwd='.')
        if result.returncode == 0:
            return result.stdout
    except:
        pass
    return None


if __name__ == "__main__":
    # 获取项目根目录（脚本所在目录）
    project_root = os.path.dirname(os.path.abspath(__file__))
    print(f"项目根目录: {project_root}\n")
    print("=" * 60)
    print("项目结构:")
    print("=" * 60)

    # 方法1：使用系统 tree 命令
    tree_output = get_tree_command()
    if tree_output:
        print(tree_output)
        save_to_file(tree_output, 'project_tree.txt')
    else:
        # 方法2：使用自定义脚本
        with open('project_structure.txt', 'w', encoding='utf-8') as f:
            f.write(f"项目根目录: {project_root}\n\n")
            print_project_structure(project_root, output_file=f)

        # 同时打印到控制台
        print_project_structure(project_root)
        print(f"\n项目结构已保存到: project_structure.txt")

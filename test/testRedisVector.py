# _*_ coding : utf-8 _*_
# @Time : 2026/2/24 19:48
# @Author : Morton
# @File : testRedisVector
# @Project : algorithm-engine


# !/usr/bin/env python3
"""
Redis Stack 向量模块快速验证
"""

import redis
import numpy as np
from redis.commands.search.field import VectorField, TextField, NumericField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# 连接Redis
r = redis.Redis(
    host='192.168.150.102',
    port=6379,
    password='123456',
    decode_responses=True
)

# 定义索引
schema = [
    TextField("content"),
    VectorField(
        "embedding",
        "HNSW",          # 算法: FLAT 或 HNSW
        {
            "TYPE": "FLOAT32",
            "DIM": 128,          # 向量维度
            "DISTANCE_METRIC": "COSINE",  # 距离: COSINE / L2 / IP
        }
    )
]

index_def = IndexDefinition(prefix=["doc:"], index_type=IndexType.HASH)

try:
    r.ft("my_index").create_index(schema, definition=index_def)
    print("索引创建成功")
except Exception as e:
    print(f"索引已存在或出错: {e}")

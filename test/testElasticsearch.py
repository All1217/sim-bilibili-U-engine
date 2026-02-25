# _*_ coding : utf-8 _*_
# @Time : 2026/2/24 16:15
# @Author : Morton
# @File : testElasticsearch
# @Project : algorithm-engine

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import numpy as np

# 连接Elasticsearch（默认密码在第一次启动时生成）
es = Elasticsearch(
    "http://192.168.150.102:9200",
    verify_certs=False  # 开发环境使用
)

# 创建索引并配置向量字段
index_name = "vector_index"

# # 索引映射配置
# mapping = {
#     "mappings": {
#         "properties": {
#             "title": {
#                 "type": "text"
#             },
#             "content": {
#                 "type": "text"
#             },
#             "embedding": {
#                 "type": "dense_vector",  # 向量字段类型
#                 "dims": 768,  # 向量维度（根据你的模型调整）
#                 "index": True,
#                 "similarity": "cosine"  # 相似度计算方式：cosine/l2/dot_product
#             },
#             "metadata": {
#                 "type": "object"
#             }
#         }
#     },
#     "settings": {
#         "number_of_shards": 1,
#         "number_of_replicas": 0
#     }
# }
# # 删除已存在的索引（如果需要）
# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)
# # 创建索引
# es.indices.create(index=index_name, body=mapping)

# 插入向量数据
def generate_sample_vector(dims=768):
    """生成示例向量（实际应用中应该使用真实的嵌入模型）"""
    return np.random.randn(dims).tolist()

# 插入单个文档
doc = {
    "title": "示例文档1",
    "content": "这是一个关于机器学习的示例文档",
    "embedding": generate_sample_vector(),
    "metadata": {
        "category": "AI",
        "tags": ["机器学习", "Python"]
    }
}

response = es.index(index=index_name, body=doc)
print(f"文档插入成功，ID: {response['_id']}")

# 批量插入文档
docs = [
    {
        "_index": index_name,
        "_source": {
            "title": f"示例文档{i}",
            "content": f"这是第{i}个示例文档的内容",
            "embedding": generate_sample_vector(),
            "metadata": {
                "category": "AI" if i % 2 == 0 else "Data",
                "tags": ["标签1", "标签2"]
            }
        }
    }
    for i in range(1, 6)
]

# 批量插入
success, _ = bulk(es, docs)
print(f"批量插入成功 {success} 条文档")

# 向量相似度搜索
def vector_search(query_vector, k=5):
    """
    向量相似度搜索
    :param query_vector: 查询向量
    :param k: 返回的最相似文档数量
    """
    search_body = {
        "size": k,
        "query": {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        },
        "_source": {"excludes": ["embedding"]}  # 不返回向量字段，减少数据量
    }
    response = es.search(index=index_name, body=search_body)
    return response['hits']['hits']


# 执行搜索
query_vector = generate_sample_vector()
results = vector_search(query_vector, k=3)

print("\n搜索结果：")
for hit in results:
    print(f"分数: {hit['_score']}, 标题: {hit['_source']['title']}")

# 使用KNN搜索（Elasticsearch 8.0+支持）
def knn_search(query_vector, k=5, num_candidates=100):
    """
    使用KNN进行向量搜索（更高效）
    :param query_vector: 查询向量
    :param k: 返回的最相似文档数量
    :param num_candidates: 每个分片考虑的候选数
    """
    search_body = {
        "size": k,
        "query": {
            "match_all": {}
        },
        "knn": {
            "field": "embedding",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": num_candidates
        },
        "_source": {"excludes": ["embedding"]}
    }

    response = es.search(index=index_name, body=search_body)
    return response['hits']['hits']


# 执行KNN搜索
knn_results = knn_search(query_vector, k=3)

print("\nKNN搜索结果：")
for hit in knn_results:
    print(f"分数: {hit['_score']}, 标题: {hit['_source']['title']}")

# 混合搜索（向量+文本）
def hybrid_search(query_text, query_vector, k=5):
    """
    混合搜索：结合文本相关性和向量相似度
    """
    search_body = {
        "size": k,
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "content": query_text
                        }
                    },
                    {
                        "script_score": {
                            "query": {"match_all": {}},
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                                "params": {"query_vector": query_vector}
                            }
                        }
                    }
                ]
            }
        }
    }

    response = es.search(index=index_name, body=search_body)
    return response['hits']['hits']


# 执行混合搜索
hybrid_results = hybrid_search("机器学习", query_vector, k=3)

print("\n混合搜索结果：")
for hit in hybrid_results:
    print(f"分数: {hit['_score']}, 标题: {hit['_source']['title']}")
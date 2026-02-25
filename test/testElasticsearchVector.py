# _*_ coding : utf-8 _*_
# @Time : 2026/2/24 21:39
# @Author : Morton
# @File : testElasticsearchVector
# @Project : algorithm-engine

import numpy as np
from elasticsearch import Elasticsearch

def generate_sample_vector(dims=768):
    """生成示例向量（实际应用中应该使用真实的嵌入模型）"""
    return np.random.randn(dims).tolist()

class VectorElasticsearch:
    def __init__(self, host="https://localhost:9200"):
        self.es = Elasticsearch(
            host,
            verify_certs=False
        )
        self.index_name = None

    def create_vector_index(self, index_name, vector_dims=768, similarity="cosine"):
        """创建向量索引"""
        self.index_name = index_name

        mapping = {
            "mappings": {
                "properties": {
                    "embedding": {
                        "type": "dense_vector",
                        "dims": vector_dims,
                        "index": True,
                        "similarity": similarity
                    }
                }
            }
        }

        # 允许动态添加其他字段
        mapping["mappings"]["dynamic"] = True

        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, body=mapping)
            print(f"索引 {index_name} 创建成功")
        else:
            print(f"索引 {index_name} 已存在")

    def insert_document(self, embedding, **kwargs):
        """插入文档"""
        doc = kwargs
        doc["embedding"] = embedding
        return self.es.index(index=self.index_name, body=doc)

    def search_by_vector(self, query_vector, k=10):
        """向量搜索"""
        body = {
            "size": k,
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {"query_vector": query_vector}
                    }
                }
            }
        }
        return self.es.search(index=self.index_name, body=body)


# 使用示例
v_es = VectorElasticsearch()
v_es.create_vector_index("my_vectors", vector_dims=768)

# 插入文档
v_es.insert_document(
    embedding=generate_sample_vector(),
    title="测试文档",
    content="这是测试内容"
)

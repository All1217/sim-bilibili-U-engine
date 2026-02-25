# _*_ coding : utf-8 _*_
# @Time : 2026/2/25 14:45
# @Author : Morton
# @File : vectorization.py
# @Project : algorithm-engine

from typing import List, Dict, Any
from src.util.database import getES
from src.util.jsonHandler import loadJson
from src.util.llmClient import get_embedding_client, embed_text


class TagVectorManager:
    """标签向量管理器（向量数据库操作）"""

    def __init__(self):
        self.es = getES()
        self.index_name = "tag_vectors"
        self.embedding_client = get_embedding_client()  # 使用共享客户端
        self.dimension = self.embedding_client.get_embedding_dimension()
        self._init_index()

    def _init_index(self):
        """初始化ES索引（如果不存在）"""
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "tag_name": {
                            "type": "keyword"
                        },
                        "category": {
                            "type": "keyword"
                        },
                        "keywords": {
                            "type": "text"
                        },
                        "vector": {
                            "type": "dense_vector",
                            "dims": self.dimension,
                            "similarity": "cosine",
                            "index": True,
                            "index_options": {
                                "type": "hnsw",
                                "m": 16,
                                "ef_construction": 100
                            }
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval": "30s"
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)
            print(f"✅ 创建索引: {self.index_name} (维度: {self.dimension})")

    def precompute_all_tags(self, tags_dict: Dict[str, List[str]] = None):
        """
        预计算所有标签的向量并存入ES
        Args:
            tags_dict: 标签字典，如果为None则从tags.json加载
        """
        if tags_dict is None:
            tags_dict = loadJson('tags.json')
        print(f"🚀 开始预计算标签向量，共 {len(tags_dict)} 个标签...")
        for category, keywords in tags_dict.items():
            # 将类别和关键词合并成一段描述文本
            tag_text = f"{category}相关的词汇：{', '.join(keywords[:10])}"
            try:
                vector = embed_text(tag_text, text_type="document")  # 向量化
                # 准备ES文档
                doc = {
                    "tag_name": category,
                    "category": category,
                    "keywords": keywords,
                    "vector": vector
                }
                self.es.index(index=self.index_name, id=category, body=doc)  # 使用标签名作为文档ID
                print(f"  ✅ {category} 向量化完成")
            except Exception as e:
                print(f"  ❌ {category} 向量化失败: {e}")
        # 刷新索引确保数据可查
        self.es.indices.refresh(index=self.index_name)
        print(f"✅ 所有标签向量化完成，已存入ES索引 {self.index_name}")

    def search_similar_tags(self, text: str, top_k: int = 5) -> Dict[str, float]:
        """
        搜索与输入文本语义相似的标签
        Args:
            text: 输入文本
            top_k: 返回前k个最相似的标签
        Returns:
            {tag_name: similarity_score, ...}
        """
        try:
            # 1. 将输入文本向量化（使用query类型）
            text_vector = embed_text(text, text_type="query")
            # 2. ES向量检索
            query = {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                        "params": {"query_vector": text_vector}
                    }
                }
            }
            result = self.es.search(
                index=self.index_name,
                body={"query": query, "size": top_k},
                _source_includes=["tag_name"]
            )
            # 3. 解析结果
            matches = {}
            for hit in result['hits']['hits']:
                tag_name = hit['_source']['tag_name']
                similarity = hit['_score'] - 1.0  # 还原余弦相似度
                matches[tag_name] = round(similarity, 4)
            return matches
        except Exception as e:
            print(f"❌ 向量检索失败: {e}")
            return {}

    def delete_index(self):
        """删除索引（重置用）"""
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
            print(f"✅ 删除索引: {self.index_name}")

    def get_index_stats(self) -> Dict[str, Any]:
        """获取索引统计信息"""
        stats = self.es.indices.stats(index=self.index_name)
        count = self.es.count(index=self.index_name)
        return {
            "index_name": self.index_name,
            "document_count": count['count'],
            "store_size": stats['_all']['total']['store']['size_in_bytes'],
            "health": self.es.cluster.health(index=self.index_name)['status'],
            "dimension": self.dimension
        }


_tag_vector_manager = None


def getTagVectorManager():
    global _tag_vector_manager
    if _tag_vector_manager is None:
        _tag_vector_manager = TagVectorManager()
    return _tag_vector_manager


if __name__ == "__main__":
    manager = getTagVectorManager()
    # 预计算所有标签
    print("\n🚀 开始预计算标签向量...")
    tags_dict = loadJson('tags.json')
    manager.precompute_all_tags(tags_dict)
    # 显示统计信息
    stats = manager.get_index_stats()
    print(f"\n📊 ES索引统计:")
    print(f"  - 索引名: {stats['index_name']}")
    print(f"  - 文档数: {stats['document_count']}")
    print(f"  - 存储大小: {stats['store_size']} 字节")
    print(f"  - 健康状态: {stats['health']}")
    print(f"  - 向量维度: {stats['dimension']}")

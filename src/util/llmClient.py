# _*_ coding : utf-8 _*_
# @Time : 2026/2/25 14:57
# @Author : Morton
# @File : llmClient
# @Project : algorithm-engine

import os
from openai import OpenAI
import threading
from typing import List, Optional, Dict, Any
import src.config.application as config


class BailianEmbeddingClient:
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self._init_client()
                    self._initialized = True

    def _init_client(self):
        print("🚀 初始化阿里百炼大模型客户端...")
        self.api_key = config.DASHSCOPE_API_KEY
        if not self.api_key:
            raise ValueError("❌ 环境变量 DASHSCOPE_API_KEY 未设置")
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=config.MODEL_HOST,
            timeout=30
        )
        self.default_model = config.DEFAULT_MODEL
        self.default_dimension = config.MODEL_DIMENSION  # 维度
        self.max_batch_size = 20  # 最大批量处理数量
        print("✅ 阿里百炼大模型客户端初始化完成")

    def embed_text(self, text: str,
                   model: str = None,
                   dimension: int = None,
                   text_type: str = "document") -> List[float]:
        """
        将单条文本向量化
        Args:
            text: 输入文本
            model: 模型名称，默认使用text-embedding-v4
            dimension: 向量维度，默认1024
            text_type: query 或 document
        Returns:
            向量列表
        """
        try:
            resp = self.client.embeddings.create(
                model=model or self.default_model,
                input=[text],
                dimensions=dimension or self.default_dimension,
                encoding_format="float"
            )
            return resp.data[0].embedding
        except Exception as e:
            print(f"❌ 向量化失败: {e}")
            raise

    def embed_batch(self, texts: List[str],
                    model: str = None,
                    dimension: int = None,
                    text_type: str = "document") -> List[List[float]]:
        """
        批量向量化多条文本
        Args:
            texts: 输入文本列表
            model: 模型名称
            dimension: 向量维度
            text_type: query 或 document
        Returns:
            向量列表，顺序与输入一致
        """
        if not texts:
            return []
        try:
            resp = self.client.embeddings.create(
                model=model or self.default_model,
                input=texts,
                dimensions=dimension or self.default_dimension,
                encoding_format="float"
            )
            # 按输入顺序返回向量
            embeddings = [None] * len(texts)
            for data in resp.data:
                embeddings[data.index] = data.embedding
            return embeddings
        except Exception as e:
            print(f"❌ 批量向量化失败: {e}")
            raise

    def get_embedding_dimension(self) -> int:
        """获取当前向量维度"""
        return self.default_dimension

    def set_default_dimension(self, dimension: int):
        """设置默认向量维度"""
        if dimension not in [256, 512, 768, 1024, 1536, 2048]:
            print(f"⚠️ 警告: {dimension} 可能不是标准维度")
        self.default_dimension = dimension
        print(f"✅ 默认向量维度已设置为: {dimension}")

    def health_check(self) -> bool:
        """健康检查"""
        try:
            self.embed_text("健康检查", text_type="document")
            return True
        except Exception:
            return False


# 创建全局单例
_embedding_client = None


def get_embedding_client() -> BailianEmbeddingClient:
    """获取向量化客户端单例"""
    global _embedding_client
    if _embedding_client is None:
        _embedding_client = BailianEmbeddingClient()
    return _embedding_client


def embed_text(text: str, text_type: str = "document", **kwargs) -> List[float]:
    """便捷函数：单条文本向量化"""
    return get_embedding_client().embed_text(text, text_type=text_type, **kwargs)


def embed_batch(texts: List[str], text_type: str = "document", **kwargs) -> List[List[float]]:
    """便捷函数：批量文本向量化"""
    return get_embedding_client().embed_batch(texts, text_type=text_type, **kwargs)


if __name__ == "__main__":
    print("=" * 60)
    print("🎯 测试阿里百炼大模型客户端")
    print("=" * 60)

    client = get_embedding_client()

    # 测试健康检查
    if client.health_check():
        print("✅ 大模型服务连接正常")
    else:
        print("❌ 大模型服务连接失败")
        exit(1)

    # 测试单条向量化
    test_text = "今天天气真好"
    vector = embed_text(test_text)
    print(f"\n📝 输入文本: {test_text}")
    print(f"📊 向量维度: {len(vector)}")
    print(f"📊 向量前5维: {vector[:5]}")

    # 测试批量向量化
    test_texts = [
        "考研数学太难了",
        "这个显卡性能真强",
        "火锅很好吃"
    ]
    vectors = embed_batch(test_texts)
    print(f"\n📝 批量处理 {len(vectors)} 条文本")
    for i, vec in enumerate(vectors):
        print(f"  {i + 1}. {test_texts[i][:10]}... 维度: {len(vec)}")

    # 测试不同维度
    client.set_default_dimension(768)
    vector_768 = embed_text("测试不同维度")
    print(f"\n📊 768维度向量前5维: {vector_768[:5]}")

    # 恢复默认
    client.set_default_dimension(1024)

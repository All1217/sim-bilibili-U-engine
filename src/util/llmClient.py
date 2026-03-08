# _*_ coding : utf-8 _*_
# @Time : 2026/2/25 14:57
# @Author : Morton
# @File : llmClient
# @Project : algorithm-engine

from openai import OpenAI
import threading
from typing import List
import src.config.application as config
import logging


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
        批量向量化多条文本（分块处理，不是真正意义上的异步批处理）
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
        BATCH_SIZE = 10
        all_embeddings = [None] * len(texts)
        try:
            # 分批处理
            for i in range(0, len(texts), BATCH_SIZE):
                batch_texts = texts[i:i + BATCH_SIZE]
                # 记录这批文本在原列表中的索引范围
                start_idx = i
                end_idx = min(i + BATCH_SIZE, len(texts))
                # 调用API处理这一批
                resp = self.client.embeddings.create(
                    model=model or self.default_model,
                    input=batch_texts,
                    dimensions=dimension or self.default_dimension,
                    encoding_format="float"
                )
                # 将结果放回正确的位置
                for data in resp.data:
                    original_index = start_idx + data.index
                    all_embeddings[original_index] = data.embedding
            return all_embeddings
        except Exception as e:
            print(f"❌ 批量向量化出错: {e}")
            logging.exception(e)
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

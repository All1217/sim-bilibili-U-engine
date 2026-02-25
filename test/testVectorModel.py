# _*_ coding : utf-8 _*_
# @Time : 2026/2/24 22:22
# @Author : Morton
# @File : testVectorModel
# @Project : algorithm-engine

import os
from openai import OpenAI
from elasticsearch import Elasticsearch

es = Elasticsearch(
    "http://192.168.150.102:9200",
    verify_certs=False  # 开发环境使用
)

input_text = "衣服的质量杠杠的"

client = OpenAI(
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

completion = client.embeddings.create(
    model="text-embedding-v4",
    input=input_text,
    dimensions=1024
)
vector = completion.data[0].embedding
doc = {
    "title": "商品评论",
    "content": input_text,
    "embedding": vector,  # 存储768维向量
    "metadata": {
        "source": "阿里云百炼",
        "model": "text-embedding-v4"
    }
}

response = es.index(index="product_reviews", body=doc)
print(f"保存成功，ID: {response['_id']}")

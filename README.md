# AniLink-用户画像引擎

基于Flask框架构建的用户画像服务，与Java后端服务主要的通信方式为：

1. 共享Redis缓存
2. 收发RabbitMQ消息
3. 通过符合RESTful api规范的接口调用服务

# 1 环境配置

推荐克隆main分支，其他分支均为测试。

克隆：`git clone -b main https://github.com/All1217/AniLink-AlgorithmEngine.git`

## 1.1 如果没有anaconda

进入根目录，创建虚拟环境`python -m venv .venv`

激活虚拟环境（Windows，并且你的python版本要大于等于3.3）：`.venv\Scripts\activate`

看到 (.venv) 提示符后安装依赖：`pip install -r requirements.txt`

## 1.2 如果有anaconda

酌情选用你的anaconda环境即可

# 2 启动

找到`根目录名/src/main.py`，右击启动，或者进入该目录后在命令行输入：`python main.py`

# AniLink-用户画像引擎

基于Flask框架构建的用户画像服务，与Java后端服务主要的通信方式为：

1. 共享Redis缓存
2. 收发RabbitMQ消息
3. 通过符合RESTful api规范的接口调用服务


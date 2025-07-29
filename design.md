## 消息消费

### 1. 消息怎么投递给消费者
 长轮询时的4个细节处理

- 客户端请求异步化
`
导致很多queue时，数据延迟是很大的。
PushConsumer并没有往多线程的思路实现，而是把Pull请求做成了异步化，减小寄生在客户端上的负担。DefaultLitePullConsumerImpl底层的多队列实现则是多线程+同步的方式进行拉取
`

- 服务端超时处理细节
`
无消息到达时，防止客户端会产生大量超时，定时处理返回PULL_NOT_FOUND
`
 
- 服务端多线程交互
`
  服务端逻辑散落在不同的线程处理上
pull线程、put线程，处理消息写入、holdCheck线程，定期任务检查超时时间
PullRequestHoldService，涉及多个线程池之间的线程唤醒
`

- 合并多个Pull请求
`
 服务端合并成一个mpr ManyPullRequest，管理到queue维度并非topic维度
 有消息到了能一下子找到对应的pull请求背后的连接去写响应包
`

### 2. 消费者如何消费消息

4个线程池参与核心的工作
- PullMessageServiceScheduledThread
- 回调线程池
- 消费线程池
- MQClientFactoryScheduledThread，持续把内存的消费进度定期同步给Broker

### 3. 消息进度如何管理
定时同步消费进度，就是ProcessQueue的队首的位点
针对某个queue维度去管理消费进度，不是针对消息一条一条地管理是否已经消费的，而是针对每个queue直接记录一个位点consumerOffset进行管理

### 4. 消费失败如何重试
发送到重试主题，并从ProcessQueue删除（标记为消费成功）
- 客户端消费超时，定期扫描所有消费中的消息，超过这个时间（ConsumeTimeout配置，默认15min），就会触发消息的重发到重试主题
- 卡进度的保护处理，consumeConcurrentlyMaxSpan默认2000，为了减少异常场景下卡进度的影响，发生流控而暂停拉取消息。即便真的发生了重启，最多也只会重复2000条消息

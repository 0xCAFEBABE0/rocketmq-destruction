package org.gnor.rocketmq.cretry_rNPC_14.consumer;

import org.gnor.rocketmq.common_1.MixAll;
import org.gnor.rocketmq.common_1.TopicRouteData;
import org.gnor.rocketmq.cretry_rNPC_14.consumer.listener.MessageListenerConcurrently;
import org.gnor.rocketmq.cretry_rNPC_14.consumer.store.RemoteBrokerOffsetStore;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultMQPushConsumer {
    private PullMessageService pullMessageService = new PullMessageService(this);
    private MessageListenerConcurrently messageListener;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));

    /* release_1：消费进度存储*/
    private RemoteBrokerOffsetStore remoteBrokerOffsetStore = new RemoteBrokerOffsetStore(this);

    /* 14-cRetry: 消费重试 */
    private String consumerGroup;
    private int maxReconsumeTimes = 16;

    public DefaultMQPushConsumer(String consumerGroup, MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.consumerGroup = consumerGroup;
    }

    public RemoteBrokerOffsetStore getOffsetStore() {
        return remoteBrokerOffsetStore;
    }
    public String getConsumerGroup() {
        return consumerGroup;
    }
    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }


    public void start() {
        TopicRouteData topicRouteData = pullMessageService.queryTopicRouteInfo("Topic-T01");
        String topic = topicRouteData.getTopic();
        Map<String, Integer> queueTable = topicRouteData.getQueueTable();

        queueTable.forEach((k, v) -> {
            for (int i = 0; i < v; ++i) {
                MessageQueue mq = new MessageQueue(topic, k, i);
                PullRequest pullRequest = new PullRequest(mq, new ProcessQueue());
                pullMessageService.executePullRequest(pullRequest);

                MessageQueue messageQueue = pullRequest.getMessageQueue();
                this.pullMessageService.rebalanceService.addTopicSubscribeInfo(messageQueue.getTopic(), new MessageQueue( messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId()));
            }
        });
        this.copySubscription(queueTable);

        pullMessageService.sendHeartbeatToBroker(topic);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                System.out.println("发送心跳：" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                pullMessageService.sendHeartbeatToBroker(topic);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 1000, 10_000, TimeUnit.MILLISECONDS);

        new Thread(pullMessageService).start();

        this.scheduledExecutorService.scheduleAtFixedRate(this::persistConsumerOffset, 1000, 5_000, TimeUnit.MILLISECONDS);
    }

    public MessageListenerConcurrently getMessageListener() {
        return this.messageListener;
    }
    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public void persistConsumerOffset() {
        Set<MessageQueue> allocateMq = this.getPullMessageService().rebalanceService.getProcessQueueTable().keySet();
        this.getOffsetStore().persistAll(allocateMq);
    }

    /**
     * 复制订阅
     * - 注册重试主题
     *
     * @param queueTable
     */
    private void copySubscription(Map<String, Integer> queueTable) {
        String retryTopic = MixAll.getRetryTopic(this.consumerGroup);

        queueTable.forEach((k, v) -> {
            for (int i = 0; i < v; ++i) {
                MessageQueue mq = new MessageQueue(retryTopic, k, i);
                PullRequest pullRequest = new PullRequest(mq, new ProcessQueue());
                pullMessageService.executePullRequest(pullRequest);
                MessageQueue messageQueue = pullRequest.getMessageQueue();
                this.pullMessageService.rebalanceService.addTopicSubscribeInfo(messageQueue.getTopic(), new MessageQueue( messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId()));
            }
        });
    }
}

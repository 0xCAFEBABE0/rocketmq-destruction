package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.consumer;

import org.gnor.rocketmq.common_1.TopicRouteData;
import org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.consumer.listener.MessageListenerConcurrently;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultMQPushConsumer {
    private PullMessageService pullMessageService = new PullMessageService(this);
    private MessageListenerConcurrently messageListener;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));

    public DefaultMQPushConsumer(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
    }

    public void start() {
        TopicRouteData topicRouteData = pullMessageService.queryTopicRouteInfo("Topic-T01");
        String topic = topicRouteData.getTopic();
        Map<String, Integer> queueTable = topicRouteData.getQueueTable();

        queueTable.forEach((k, v) -> {
            for (int i = 0; i < v; ++i) {
                PullRequest pullRequest = new PullRequest();
                pullRequest.setTopic(topic);
                pullRequest.setBrokerName(k);
                pullRequest.setQueueId(i);
                pullMessageService.executePullRequest(pullRequest);

                this.pullMessageService.rebalanceService.addTopicSubscribeInfo(pullRequest.getTopic(), new MessageQueue( pullRequest.getTopic(), pullRequest.getBrokerName(), pullRequest.getQueueId()));
            }
        });

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
    }

    public MessageListenerConcurrently getMessageListener() {
        return this.messageListener;
    }
}

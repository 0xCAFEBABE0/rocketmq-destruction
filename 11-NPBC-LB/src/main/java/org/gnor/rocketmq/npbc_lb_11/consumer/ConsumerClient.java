package org.gnor.rocketmq.npbc_lb_11.consumer;

import org.gnor.rocketmq.common_1.TopicRouteData;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerClient {
    //private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerKeepingService"));
    //private Channel channel = null;
    //private volatile boolean isRunning = false;
    //

    private PullMessageService pullMessageService = new PullMessageService();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));

    public static void main(String[] args) throws Exception {
        new ConsumerClient().run();
    }

    public void run() throws Exception {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                pullMessageService.sendHeartbeatToBroker();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 1000, 30_000, TimeUnit.MILLISECONDS);


        TopicRouteData topicRouteData = pullMessageService.queryTopicRouteInfo("Topic-T01");
        String topic = topicRouteData.getTopic();
        Map<String, Integer> queueTable = topicRouteData.getQueueTable();

        queueTable.forEach((k, v) -> {
            for (Integer i = 0; i < v; ++i) {
                PullRequest pullRequest = new PullRequest();
                pullRequest.setTopic(topic);
                pullRequest.setBrokerName(k);
                pullRequest.setQueueId(i);
                pullMessageService.executePullRequest(pullRequest);

                this.pullMessageService.rebalanceService.addTopicSubscribeInfo(pullRequest.getTopic(), new MessageQueue( pullRequest.getTopic(), pullRequest.getBrokerName(), pullRequest.getQueueId()));
            }
        });
        new Thread(pullMessageService).start();
    }


}

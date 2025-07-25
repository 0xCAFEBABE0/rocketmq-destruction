package org.gnor.rocketmq.npbc_consumermodel_10.consumer;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.ThreadFactoryImpl;
import org.gnor.rocketmq.common_1.TopicRouteData;
import org.gnor.rocketmq.npbc_consumermodel_10.remoting.NettyRemotingClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ConsumerClient {
    //private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerKeepingService"));
    //private Channel channel = null;
    //private volatile boolean isRunning = false;
    //

    private PullMessageService pullMessageService = new PullMessageService();

    public static void main(String[] args) throws Exception {
        new ConsumerClient().run();
    }

    public void run() throws Exception {
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
            }
        });

        new Thread(pullMessageService).start();
    }

}

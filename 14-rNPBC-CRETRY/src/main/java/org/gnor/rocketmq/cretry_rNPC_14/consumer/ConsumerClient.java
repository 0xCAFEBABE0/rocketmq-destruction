package org.gnor.rocketmq.cretry_rNPC_14.consumer;

import org.gnor.rocketmq.common_1.ConsumeConcurrentlyStatus;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.TopicRouteData;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConsumerClient {
    //private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerKeepingService"));
    //private Channel channel = null;
    //private volatile boolean isRunning = false;
    //

    public static void main(String[] args) throws Exception {
       new DefaultMQPushConsumer("TeGroup-C01", (res, context) -> {
           for (RemotingCommand response : res) {
               System.out.println(response.getTopic() + "收到服务器响应消息: " + response.getHey() + " [时间: " +
                       LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]" + " [队列: " + context.getMessageQueue().getQueueId() + "]");
           }
           return ConsumeConcurrentlyStatus.RECONSUME_LATER;
       }).start();
    }

}

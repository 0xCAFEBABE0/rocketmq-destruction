package org.gnor.rocketmq.cretry_rNPC_14.consumer;

import com.alibaba.fastjson2.JSON;
import org.gnor.rocketmq.common_1.ConsumeConcurrentlyStatus;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.TopicRouteData;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class ConsumerClient {
    //private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerKeepingService"));
    //private Channel channel = null;
    //private volatile boolean isRunning = false;
    //
    static final AtomicInteger i = new AtomicInteger(12);

    public static void main(String[] args) throws Exception {
       new DefaultMQPushConsumer("TeGroup-C01", (res, context) -> {
           System.out.println("收到服务器响应消息:size:" + res.size());
           for (RemotingCommand response : res) {
               System.out.println(response.getTopic() + "收到服务器响应消息: " + response.getHey() + " [时间: " +
                       LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]" + " [队列: " + context.getMessageQueue().getQueueId() + "]," + JSON.toJSONString(response));
           }
           try {
               TimeUnit.MILLISECONDS.sleep(3_000L);
           } catch (InterruptedException e) {
               throw new RuntimeException(e);
           }
           if (i.getAndDecrement() < 0) {
               return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
           }
           return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
       }).start();
    }

}

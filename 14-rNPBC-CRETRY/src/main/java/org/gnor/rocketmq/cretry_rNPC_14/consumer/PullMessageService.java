package org.gnor.rocketmq.cretry_rNPC_14.consumer;

import com.alibaba.fastjson2.JSON;
import org.gnor.rocketmq.common_1.*;
import org.gnor.rocketmq.common_1.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.InvokeCallback;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.NettyRemotingClient;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.ResponseFuture;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PullMessageService implements Runnable {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();
    private LinkedBlockingDeque<PullRequest> pullRequestQueue = new LinkedBlockingDeque<>();

    /*v11版本新增：重平衡*/
    protected final RebalanceService rebalanceService = new RebalanceService(this);

    Map<String, String> brokerAddrTable;

    /*release_1版本新增：消费线程池*/
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final ThreadPoolExecutor consumeExecutor;

    public PullMessageService(DefaultMQPushConsumer defaultMQPushConsumer) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.consumeExecutor = new ThreadPoolExecutor(
                20,
                20,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("ConsumeMessageThread_" ));

    }

    public void executePullRequest(PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void run() {
        new Thread(rebalanceService).start();
        while (true) {
            try {
                PullRequest take = this.pullRequestQueue.take();
                this.pullMessage(take);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public TopicRouteData queryTopicRouteInfo(String topic) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.GET_ROUTEINFO_BY_TOPIC);
        remotingCommand.setTopic(topic);
        remotingCommand.setHey("Query topic route info from namesrv");

        RemotingCommand queryTopicFromNamesrv = null;
        try {
            queryTopicFromNamesrv = this.remotingClient.invokeSync("127.0.0.1:9091", remotingCommand, 30000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        TopicRouteData topicRouteData = queryTopicFromNamesrv.getTopicRouteData();
        this.brokerAddrTable = topicRouteData.getBrokerAddrTable();
        return topicRouteData;
    }

    public void pullMessage(PullRequest pullRequest) throws InterruptedException {
        if (pullRequest.isDropped()) {
            System.out.println("PullRequest is dropped, skip it");
            return;
        }

        //release_1：异步化改造
        InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                // 读取服务器发送的消息
                if (response.getFlag() == RemotingCommand.RESPONSE_FLAG) {

                    if (response.getCode() == RemotingCommand.CONSUMER_MSG) {
                        consumeExecutor.submit(() -> {

                            //tryResetPopRetryTopic(response, defaultMQPushConsumer.getConsumerGroup());

                            ProcessQueue processQueue = pullRequest.getProcessQueue();
                            List<RemotingCommand> msgs = new ArrayList<>();
                            msgs.add(response);
                            boolean b = processQueue.putMessage(msgs);

                            pullRequest.setNextOffset(response.getNextBeginOffset());
                            ConsumeConcurrentlyStatus consumeConcurrentlyStatus = defaultMQPushConsumer.getMessageListener().consumeMessage(msgs, pullRequest);

                            //源码里使用ConsumerRequest做解耦
                            /*14_cRETRY 消费端重试*/
                            if (consumeConcurrentlyStatus != ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {

                                RemotingCommand request = new RemotingCommand();
                                ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
                                request.setCustomHeader(requestHeader);
                                requestHeader.setGroup(defaultMQPushConsumer.getConsumerGroup());
                                requestHeader.setOriginTopic(response.getTopic());
                                requestHeader.setOffset(response.getCommitOffset());
                                requestHeader.setDelayLevel(0);
                                requestHeader.setOriginMsgId("");
                                requestHeader.setMaxReconsumeTimes(defaultMQPushConsumer.getMaxReconsumeTimes());

                                request.setHey(response.getHey());
                                request.setCommitOffset(response.getCommitOffset());
                                request.setFlag(RemotingCommand.REQUEST_FLAG);
                                request.setCode(RemotingCommand.CONSUMER_SEND_MSG_BACK);
                                MessageQueue messageQueue = pullRequest.getMessageQueue();
                                String brokerName = messageQueue.getBrokerName();
                                String brokerAddr = brokerAddrTable.get(brokerName);
                                try {
                                    remotingClient.invokeSync(brokerAddr, request, 30_000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            //if (consumeConcurrentlyStatus == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                            long offset = processQueue.removeMessage(msgs);
                            if (offset >= 0 && !pullRequest.isDropped()) {
                                defaultMQPushConsumer.getOffsetStore().updateOffset(pullRequest.getMessageQueue(), offset);
                            }

                        });
                    }
                } else {
                    System.out.println("收到服务器消息: " + response.getHey());
                }
                executePullRequest(pullRequest);
            }

            @Override
            public void operationFail(Throwable throwable) {
                InvokeCallback.super.operationFail(throwable);
            }
        };

        try {
            MessageQueue messageQueue = pullRequest.getMessageQueue();
            String brokerName = messageQueue.getBrokerName();
            String brokerAddr = brokerAddrTable.get(brokerName);

            //RemotingCommand getConsumerOffsetCommand = new RemotingCommand();
            //getConsumerOffsetCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            //getConsumerOffsetCommand.setCode(RemotingCommand.QUERY_CONSUMER_OFFSET);
            //getConsumerOffsetCommand.setTopic(messageQueue.getTopic());
            //getConsumerOffsetCommand.setQueueId(messageQueue.getQueueId());
            //getConsumerOffsetCommand.setConsumerGroup("ConsumerGroup-C01");
            //
            //System.out.println("发送拉取消息Offset请求: " + getConsumerOffsetCommand.getHey());
            //RemotingCommand offsetCmd = this.remotingClient.invokeSync(brokerAddr, getConsumerOffsetCommand, 30000L);

            long commitOffset = defaultMQPushConsumer.getOffsetStore().readOffset(messageQueue);
            // 发送拉取消息请求
            RemotingCommand consumerCmd = new RemotingCommand();
            consumerCmd.setFlag(RemotingCommand.REQUEST_FLAG);
            consumerCmd.setCode(RemotingCommand.CONSUMER_MSG);
            consumerCmd.setConsumerGroup("ConsumerGroup-C01");
            consumerCmd.setTopic(messageQueue.getTopic());
            consumerCmd.setQueueId(messageQueue.getQueueId());
            consumerCmd.setConsumerOffset(pullRequest.getNextOffset());
            consumerCmd.setCommitOffset(commitOffset);
            consumerCmd.setProperties(JSON.toJSONString(new HashMap<String, String>() {{
                put("TAG", "TAG-A");
            }}));
            consumerCmd.setHey("Pull message request from consumer");
            System.out.println("发送拉取消息请求: " + consumerCmd.getHey());
            this.remotingClient.invokeAsync(brokerAddr, consumerCmd, 30000L, invokeCallback);
        } finally {
            //this.executePullRequest(pullRequest);
        }

    }

    public void tryResetPopRetryTopic(RemotingCommand consumerCmd, String consumerGroup) {
        String groupTopic = MixAll.getRetryTopic(consumerGroup);
        Map<String, String> propertiesTable = JSON.parseObject(consumerCmd.getProperties(), Map.class);

        String retryTopic = propertiesTable.get("RETRY_TOPIC");
        if (retryTopic != null && groupTopic.equals(consumerCmd.getTopic())) {
            consumerCmd.setTopic(retryTopic);
        }
    }


    public void queryRemoteConsumerOffset(MessageQueue messageQueue){
        RemotingCommand getConsumerOffsetCommand = new RemotingCommand();
        getConsumerOffsetCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        getConsumerOffsetCommand.setCode(RemotingCommand.QUERY_CONSUMER_OFFSET);
        getConsumerOffsetCommand.setTopic(messageQueue.getTopic());
        getConsumerOffsetCommand.setQueueId(messageQueue.getQueueId());
        getConsumerOffsetCommand.setConsumerGroup("ConsumerGroup-C01");

        System.out.println("发送拉取消息Offset请求: " + getConsumerOffsetCommand.getHey());
        String brokerName = messageQueue.getBrokerName();
        String brokerAddr = brokerAddrTable.get(brokerName);
        try {
            RemotingCommand offsetCmd = this.remotingClient.invokeSync(brokerAddr, getConsumerOffsetCommand, 30000L);
            long consumerOffset = offsetCmd.getConsumerOffset();
            defaultMQPushConsumer.getOffsetStore().updateOffset(messageQueue, consumerOffset);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean sendHeartbeatToBroker(String topic) {
        brokerAddrTable.forEach((bn, ba) -> {
            String brokerAddr = ba;
            String clientId = buildMQClientId();

            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            remotingCommand.setCode(RemotingCommand.HEART_BEAT);
            remotingCommand.setClientId(clientId);
            remotingCommand.setTopic(topic);
            remotingCommand.setHey("Heartbeat from consumer");
            try {
                this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        //this.pullRequestQueue.forEach(pullRequest -> {
        //    String brokerName = pullRequest.getBrokerName();
        //    String brokerAddr = brokerAddrTable.get(brokerName);
        //    String clientId = buildMQClientId();
        //    String topic = pullRequest.getTopic();
        //
        //    RemotingCommand remotingCommand = new RemotingCommand();
        //    remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        //    remotingCommand.setCode(RemotingCommand.HEART_BEAT);
        //    remotingCommand.setClientId(clientId);
        //    remotingCommand.setTopic(topic);
        //    remotingCommand.setHey("Heartbeat from consumer");
        //    try {
        //        this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);
        //    } catch (InterruptedException e) {
        //        throw new RuntimeException(e);
        //    }
        //
        //});
        return true;
    }


    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append("127.0.0.1");

        sb.append("@");
        sb.append(this.instanceName);
        //if (!UtilAll.isBlank(this.unitName)) {
        //    sb.append("@");
        //    sb.append(this.unitName);
        //}
        return sb.toString();
    }

    public void updateConsumeOffsetToBroker(MessageQueue messageQueue, long offset) {
        String brokerName = messageQueue.getBrokerName();
        String brokerAddr = brokerAddrTable.get(brokerName);

        RemotingCommand updateConsumerOffsetCommand = new RemotingCommand();
        updateConsumerOffsetCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        updateConsumerOffsetCommand.setCode(RemotingCommand.UPDATE_CONSUMER_OFFSET);
        updateConsumerOffsetCommand.setTopic(messageQueue.getTopic());
        updateConsumerOffsetCommand.setQueueId(messageQueue.getQueueId());
        updateConsumerOffsetCommand.setCommitOffset(offset);
        updateConsumerOffsetCommand.setConsumerGroup("ConsumerGroup-C01");
        try {
            this.remotingClient.invokeSync(brokerAddr, updateConsumerOffsetCommand, 30000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}

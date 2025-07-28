package org.gnor.rocketmq.npbc_lb_11.consumer;

import com.alibaba.fastjson2.JSON;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.TopicRouteData;
import org.gnor.rocketmq.npbc_lb_11.remoting.NettyRemotingClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public class PullMessageService implements Runnable {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();
    private LinkedBlockingDeque<PullRequest> pullRequestQueue = new LinkedBlockingDeque<>();

    /*v11版本新增：重平衡*/
    protected final RebalanceService rebalanceService = new RebalanceService(this);

    Map<String, String> brokerAddrTable;

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
        try {
            String brokerName = pullRequest.getBrokerName();
            String brokerAddr = brokerAddrTable.get(brokerName);

            RemotingCommand getConsumerOffsetCommand = new RemotingCommand();
            getConsumerOffsetCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            getConsumerOffsetCommand.setCode(RemotingCommand.QUERY_CONSUMER_OFFSET);
            getConsumerOffsetCommand.setTopic(pullRequest.getTopic());
            getConsumerOffsetCommand.setQueueId(pullRequest.getQueueId());
            getConsumerOffsetCommand.setConsumerGroup("ConsumerGroup-C01");

            System.out.println("发送拉取消息Offset请求: " + getConsumerOffsetCommand.getHey());
            RemotingCommand offsetCmd = this.remotingClient.invokeSync(brokerAddr, getConsumerOffsetCommand, 30000L);


            // 发送拉取消息请求
            long consumeOffset = offsetCmd.getConsumerOffset();
            RemotingCommand consumerCmd = new RemotingCommand();
            consumerCmd.setFlag(RemotingCommand.REQUEST_FLAG);
            consumerCmd.setCode(RemotingCommand.CONSUMER_MSG);
            consumerCmd.setConsumerGroup("ConsumerGroup-C01");
            consumerCmd.setTopic(pullRequest.getTopic());
            consumerCmd.setQueueId(pullRequest.getQueueId());
            consumerCmd.setConsumerOffset(consumeOffset);
            consumerCmd.setProperties(JSON.toJSONString(new HashMap<String, String>() {{
                put("TAG", "TAG-A");
            }}));
            consumerCmd.setHey("Pull message request from consumer");
            System.out.println("发送拉取消息请求: " + consumerCmd.getHey());
            RemotingCommand consumerRsp = this.remotingClient.invokeSync(brokerAddr, consumerCmd, 30000L);

            // 读取服务器发送的消息
            if (consumerRsp.getFlag() == RemotingCommand.RESPONSE_FLAG) {
                System.out.println("收到服务器响应消息: " + consumerRsp.getHey() + " [时间: " +
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]" + " [队列: " + pullRequest.getQueueId() + "]");
            } else {
                System.out.println("收到服务器消息: " + consumerRsp.getHey());
            }
        } finally {
            this.executePullRequest(pullRequest);
        }

    }

    public boolean sendHeartbeatToBroker() {
        this.pullRequestQueue.forEach(pullRequest -> {
            String brokerName = pullRequest.getBrokerName();
            String brokerAddr = brokerAddrTable.get(brokerName);
            String clientId = buildMQClientId();
            String topic = pullRequest.getTopic();

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


}

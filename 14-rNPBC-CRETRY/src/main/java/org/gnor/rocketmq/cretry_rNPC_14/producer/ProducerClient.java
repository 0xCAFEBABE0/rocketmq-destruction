package org.gnor.rocketmq.cretry_rNPC_14.producer;

import com.alibaba.fastjson2.JSON;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.ThreadLocalIndex;
import org.gnor.rocketmq.common_1.TopicRouteData;
import org.gnor.rocketmq.cretry_rNPC_14.producer.latency.MQFaultStrategy;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.NettyRemotingClient;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.RemotingSysResponseCode;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.ResponseCode;

import java.util.*;

public class ProducerClient {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();

    public static void main(String[] args) throws Exception {
        new ProducerClient().run();
    }

    //private TopicRouteData tryToFindTopicPublishInfo(final String topic) {
    //
    //}

    public void run() throws Exception {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.GET_ROUTEINFO_BY_TOPIC);
        remotingCommand.setTopic("Topic-T01");
        remotingCommand.setHey("Query topic route info from namesrv");

        RemotingCommand queryTopicFromNamesrv = this.remotingClient.invokeSync("127.0.0.1:9091", remotingCommand, 30000L);
        if (queryTopicFromNamesrv.getCode() == RemotingCommand.TOPIC_NOT_EXIST) {
            remotingCommand = new RemotingCommand();
            remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            remotingCommand.setCode(RemotingCommand.GET_ROUTEINFO_BY_TOPIC);
            remotingCommand.setTopic("TBW102");
            remotingCommand.setHey("Query topic route info from namesrv");
            queryTopicFromNamesrv = this.remotingClient.invokeSync("127.0.0.1:9091", remotingCommand, 30000L);

            queryTopicFromNamesrv.getTopicRouteData().setTopic("Topic-T01"); //偷梁换柱，借用TBW102的路由信息
        }
        this.sendDefaultImpl(queryTopicFromNamesrv, remotingCommand);

        //remotingCommand.setTopic("Topic-T02");
        //remotingCommand.setHey("Hello, Producer-02 Server!");
        //remotingCommand.setQueueId(queueId);
        //remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
        //    put("TAG", "TAG-A");
        //}}));
        //this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);
    }

    private final Random random = new Random();

    private SendResult sendDefaultImpl(RemotingCommand queryTopicFromNamesrv, RemotingCommand remotingCommand) throws InterruptedException {
        TopicRouteData topicRouteData = queryTopicFromNamesrv.getTopicRouteData();

        String topic = topicRouteData.getTopic();
        Map<String, String> brokerAddrTable = topicRouteData.getBrokerAddrTable();
        Map<String, Integer> queueTable = topicRouteData.getQueueTable();

        //v11版本新增：负载均衡
        List<MessageQueue> mqList = new ArrayList<>();
        queueTable.forEach((k, v) -> {
            for (Integer i = 0; i < v; ++i) {
                MessageQueue messageQueue = new MessageQueue(topic, k, i);
                //TODO@ch 加入负载均衡
                mqList.add(messageQueue);
            }
        });

        //生产端重试机制
        final long invokeID = random.nextLong();
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp = beginTimestampFirst;
        MessageQueue mq = null;

        int timesTotal = 3;
        int timeout = 10_000;
        int times = 0;
        boolean callTimeout = false;
        String[] brokersSent = new String[timesTotal];
        SendResult sendResult = null;
        for (; times < timesTotal; ++times) {
            String lastBrokerName = null == mq ? null : mq.getBrokerName();

            mq = mqFaultStrategy.selectOnMessageQueue(mqList, lastBrokerName);
            brokersSent[times] = mq.getBrokerName();
            //超时
            beginTimestampPrev = System.currentTimeMillis();
            long costTime = beginTimestampPrev - beginTimestampFirst;
            if (timeout < costTime) {
                callTimeout = true;
                break;
            }
            long curTimeout = timeout - costTime;

            sendResult = sendKernelImpl(remotingCommand, mq, brokerAddrTable);
            endTimestamp = System.currentTimeMillis();
            mqFaultStrategy.updateFaultItem(mq.getBrokerName(), endTimestamp -beginTimestampPrev + 5000, true);
            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                continue;
            }
        }

        if (sendResult != null) {
            return sendResult;
        }
        String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                times,
                System.currentTimeMillis() - beginTimestampFirst,
                mq.getTopic(),
                Arrays.toString(brokersSent));
        System.out.println(info);

        if (callTimeout) {
            throw new RuntimeException("sendDefaultImpl call timeout");
        }

        throw new RuntimeException("No route info of this topic: " + mq.getTopic());
    }



    private SendResult sendKernelImpl(RemotingCommand remotingCommand, MessageQueue mq, Map<String, String> brokerAddrTable) throws InterruptedException {
        String brokerName = mq.getBrokerName();
        int queueId = mq.getQueueId();
        String brokerAddr = brokerAddrTable.get(brokerName);

        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.PRODUCER_MSG);
        remotingCommand.setTopic("Topic-T01");
        remotingCommand.setQueueId(queueId);
        remotingCommand.setHey("Hello, Producer-01 Server!");
        remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
            put("TAG", "TAG-A");
            //put("DELAY", "2");
        }}));
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);

        SendStatus sendStatus;
        switch (response.getCode()) {
            case RemotingSysResponseCode.SUCCESS:
                sendStatus = SendStatus.SEND_OK;
            break;
            case ResponseCode.FLUSH_DISK_TIMEOUT:
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            case ResponseCode.SLAVE_NOT_AVAILABLE:
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            default:
                throw new RuntimeException("Unknown response code: " + response.getCode());
        }
        return new SendResult(sendStatus);
    }

    public class MessageQueue {
        private String topic;
        private String brokerName;
        private int queueId;

        public MessageQueue(String topic, String brokerName, int queueId) {
            this.topic = topic;
            this.brokerName = brokerName;
            this.queueId = queueId;
        }

        public String getTopic() {
            return topic;
        }
        public String getBrokerName() {
            return brokerName;
        }
        public int getQueueId() {
            return queueId;
        }
    }

}

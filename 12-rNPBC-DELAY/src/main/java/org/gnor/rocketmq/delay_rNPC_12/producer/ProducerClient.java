package org.gnor.rocketmq.delay_rNPC_12.producer;

import com.alibaba.fastjson2.JSON;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.ThreadLocalIndex;
import org.gnor.rocketmq.common_1.TopicRouteData;
import org.gnor.rocketmq.delay_rNPC_12.remoting.NettyRemotingClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ProducerClient {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();

    private ThreadLocalIndex sendQueue = new ThreadLocalIndex();

    public static void main(String[] args) throws Exception {
        new ProducerClient().run();
    }

    public void run() throws Exception {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.GET_ROUTEINFO_BY_TOPIC);
        remotingCommand.setTopic("Topic-T01");
        remotingCommand.setHey("Query topic route info from namesrv");

        RemotingCommand queryTopicFromNamesrv = this.remotingClient.invokeSync("127.0.0.1:9091", remotingCommand, 30000L);
        TopicRouteData topicRouteData = queryTopicFromNamesrv.getTopicRouteData();

        String topic = topicRouteData.getTopic();
        Map<String, String> brokerAddrTable = topicRouteData.getBrokerAddrTable();
        Map<String, Integer> queueTable = topicRouteData.getQueueTable();

        //v11版本新增：负载均衡
        ArrayList<Object> mqList = new ArrayList<>();
        queueTable.forEach((k, v) -> {
            for (Integer i = 0; i < v; ++i) {
                MessageQueue messageQueue = new MessageQueue(topic, k, i);
                //TODO@ch 加入负载均衡
                mqList.add(messageQueue);
            }
        });

        int index = Math.abs(this.sendQueue.incrementAndGet() % mqList.size());
        MessageQueue mq = (MessageQueue) mqList.get(index);

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
            put("DELAY", "2");
        }}));
        this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);

        //remotingCommand.setTopic("Topic-T02");
        //remotingCommand.setHey("Hello, Producer-02 Server!");
        //remotingCommand.setQueueId(queueId);
        //remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
        //    put("TAG", "TAG-A");
        //}}));
        //this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);
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

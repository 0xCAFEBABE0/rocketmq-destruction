package org.gnor.rocketmq.npbc_namesrv_8.consumer;

import com.alibaba.fastjson2.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gnor.rocketmq.common_1.*;
import org.gnor.rocketmq.npbc_namesrv_8.remoting.NettyRemotingClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ConsumerClient {
    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerKeepingService"));
    private Channel channel = null;
    private volatile boolean isRunning = false;
    private NettyRemotingClient remotingClient = new NettyRemotingClient();

    public static void main(String[] args) throws Exception {
        new ConsumerClient().run();
    }

    public void run() throws Exception {
        while (true)  {
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

            //取第一个value
            String brokerAddr = brokerAddrTable.values().iterator().next();
            RemotingCommand getConsumerOffsetCommand = new RemotingCommand();
            getConsumerOffsetCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            getConsumerOffsetCommand.setCode(RemotingCommand.QUERY_CONSUMER_OFFSET);
            getConsumerOffsetCommand.setTopic("Topic-T01");
            getConsumerOffsetCommand.setConsumerGroup("ConsumerGroup-C01");

            System.out.println("发送拉取消息Offset请求: " + getConsumerOffsetCommand.getHey());
            RemotingCommand offsetCmd = this.remotingClient.invokeSync(brokerAddr, getConsumerOffsetCommand, 30000L);


            // 发送拉取消息请求
            long consumeOffset = offsetCmd.getConsumerOffset();
            RemotingCommand consumerCmd = new RemotingCommand();
            consumerCmd.setFlag(RemotingCommand.REQUEST_FLAG);
            consumerCmd.setCode(RemotingCommand.CONSUMER_MSG);
            consumerCmd.setConsumerGroup("ConsumerGroup-C01");
            consumerCmd.setTopic("Topic-T01");
            consumerCmd.setConsumerOffset(consumeOffset);
            consumerCmd.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
                put("TAG", "TAG-A");
            }}));
            consumerCmd.setHey("Pull message request from consumer");
            System.out.println("发送拉取消息请求: " + consumerCmd.getHey());
            RemotingCommand consumerRsp = this.remotingClient.invokeSync(brokerAddr, consumerCmd, 30000L);

            // 读取服务器发送的消息
            if (consumerRsp.getFlag() == RemotingCommand.RESPONSE_FLAG) {
                System.out.println("收到服务器响应消息: " + consumerRsp.getHey() + " [时间: " +
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]");
            } else {
                System.out.println("收到服务器消息: " + consumerRsp.getHey());
            }
        }
    }

}

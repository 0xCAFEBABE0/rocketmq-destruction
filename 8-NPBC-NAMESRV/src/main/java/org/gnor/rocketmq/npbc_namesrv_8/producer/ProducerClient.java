package org.gnor.rocketmq.npbc_namesrv_8.producer;

import com.alibaba.fastjson2.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gnor.rocketmq.common_1.*;
import org.gnor.rocketmq.npbc_namesrv_8.remoting.NettyRemotingClient;

import java.util.HashMap;
import java.util.Map;

public class ProducerClient {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();

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

        //取第一个value
        String brokerAddr = brokerAddrTable.values().iterator().next();

        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.PRODUCER_MSG);
        remotingCommand.setTopic("Topic-T01");
        remotingCommand.setHey("Hello, Producer-01 Server!");
        remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
            put("TAG", "TAG-A");
        }}));
        this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);

        remotingCommand.setTopic("Topic-T02");
        remotingCommand.setHey("Hello, Producer-02 Server!");
        remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
            put("TAG", "TAG-A");
        }}));
        this.remotingClient.invokeSync(brokerAddr, remotingCommand, 30000L);
    }


}

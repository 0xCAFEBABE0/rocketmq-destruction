package org.gnor.rocketmq.order_rNPC_15.broker.processor;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.order_rNPC_15.broker.BrokerStartup;
import org.gnor.rocketmq.order_rNPC_15.broker.store.SuspendRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class PullMessageProcessor  {
    private BrokerStartup brokerStartup;

    public PullMessageProcessor(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void processRequest(final Channel channel, RemotingCommand remotingCommand) {
        String topic = remotingCommand.getTopic();
        SuspendRequest sr = new SuspendRequest(channel, remotingCommand, System.currentTimeMillis(),
                remotingCommand.getConsumerOffset(), remotingCommand.getQueueId(), remotingCommand.getOpaque());
        //todo 可移到RequestHoldService统一管理
        ConcurrentMap<String, List<SuspendRequest>> suspendRequests = this.brokerStartup.getRequestHoldService().getSuspendRequests();
        String key = SuspendRequest.buildKey(topic, remotingCommand.getQueueId());
        List<SuspendRequest> suspendRequestList = suspendRequests.getOrDefault(key, new ArrayList<>());
        suspendRequestList.add(sr);
        System.out.println("PullMessageProcessor, key:" + key + ", srList: " + JSON.toJSONString(suspendRequestList));
        suspendRequests.put(key, suspendRequestList);
    }
}

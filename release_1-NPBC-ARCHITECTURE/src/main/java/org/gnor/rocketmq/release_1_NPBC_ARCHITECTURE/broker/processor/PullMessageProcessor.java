package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker.processor;

import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker.BrokerStartup;
import org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker.store.SuspendRequest;

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
        List<SuspendRequest> suspendRequestList = suspendRequests.getOrDefault(SuspendRequest.buildKey(topic, remotingCommand.getQueueId()), new ArrayList<>());
        suspendRequestList.add(sr);
        suspendRequests.put(SuspendRequest.buildKey(topic, remotingCommand.getQueueId()), suspendRequestList);
    }
}

package org.gnor.rocketmq.cretry_rNPC_14.broker.processor;

import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.cretry_rNPC_14.broker.BrokerStartup;

public class SendBackProcessor {
    private BrokerStartup brokerStartup;

    public SendBackProcessor(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void processRequest(final Channel channel, RemotingCommand remotingCommand) {
        String topic = remotingCommand.getTopic();


    }
}

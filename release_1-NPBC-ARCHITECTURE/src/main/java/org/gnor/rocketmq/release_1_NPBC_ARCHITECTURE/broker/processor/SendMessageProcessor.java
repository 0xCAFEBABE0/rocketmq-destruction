package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker.processor;

import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker.BrokerStartup;

public class SendMessageProcessor {

    private BrokerStartup brokerStartup;
    public SendMessageProcessor(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void processRequest(final Channel channel, RemotingCommand remotingCommand, RemotingCommand response) {
        String topic = remotingCommand.getTopic();
        this.brokerStartup.getMessageStore().appendMessage(topic, remotingCommand.getHey(), remotingCommand.getProperties(), remotingCommand.getQueueId());
        /*迭代为文件存储*/
        //List<RemotingCommand> storeList = storeTopicRecord.getOrDefault(topic, new ArrayList<>());
        //storeList.add(remotingCommand);
        //storeTopicRecord.put(topic, storeList);

        //storeMSG.add(remotingCommand); //存储消息
        response.setFlag(RemotingCommand.RESPONSE_FLAG);
        response.setHey("Response echo!!!!");
        channel.writeAndFlush(response);
        //requestHoldService.notifyMessageArriving(topic);
    }
}

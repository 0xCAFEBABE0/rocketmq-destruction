package org.gnor.rocketmq.cretry_rNPC_14.broker.processor;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.MixAll;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.gnor.rocketmq.cretry_rNPC_14.broker.BrokerStartup;
import org.gnor.rocketmq.cretry_rNPC_14.broker.store.MessageStore;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.ResponseCode;

import java.util.Map;

public class SendBackProcessor {
    private BrokerStartup brokerStartup;

    public SendBackProcessor(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void processRequest(final Channel channel, RemotingCommand request) {
        String topic = request.getTopic();
        ConsumerSendMsgBackRequestHeader header = request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        String retryTopic = MixAll.getRetryTopic(header.getGroup());

        MessageStore.StoredMessage storedMessage = this.brokerStartup.getMessageStore().lookMessageByOffset(request.getCommitOffset().intValue());
        Map<String, String> properties = storedMessage.getProperties();
        properties.putIfAbsent("RETRY_TOPIC", storedMessage.getTopic());
        properties.put("DELAY", String.valueOf(3 + storedMessage.getReconsumeTimes()));

        //处理延迟消息
        String delay = properties.get("DELAY");
        if (null != delay) {
            int delayTimeLevel = Integer.parseInt(delay);
            properties.put("REAL_TOPIC", topic);
            properties.put("REAL_QID", String.valueOf(request.getQueueId()));
            request.setTopic("SCHEDULE_TOPIC_XXXX");
            request.setQueueId(delayTimeLevel - 1);
        }

        this.brokerStartup.getMessageStore().appendMessage(retryTopic, request.getHey(), JSON.toJSONString(properties), 1, storedMessage.getReconsumeTimes() + 1);

        request.setFlag(RemotingCommand.RESPONSE_FLAG);
        request.setCode(ResponseCode.SUCCESS);
        request.setHey("Send back msg success!");
        channel.writeAndFlush(request);
    }
}

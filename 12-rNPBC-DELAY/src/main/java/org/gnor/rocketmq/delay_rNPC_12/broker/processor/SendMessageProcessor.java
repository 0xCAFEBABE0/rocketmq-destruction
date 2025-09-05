package org.gnor.rocketmq.delay_rNPC_12.broker.processor;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.delay_rNPC_12.broker.BrokerStartup;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SendMessageProcessor {

    private BrokerStartup brokerStartup;
    public SendMessageProcessor(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void processRequest(final Channel channel, RemotingCommand remotingCommand, RemotingCommand response) {
        String topic = remotingCommand.getTopic();

        //处理延迟消息
        Map<String, String> pMapper = JSONObject.parseObject(remotingCommand.getProperties(), HashMap.class);
        String delay = pMapper.get("DELAY");
        if (null != delay) {
            int delayTimeLevel = Integer.parseInt(delay);
            pMapper.put("REAL_TOPIC", topic);
            pMapper.put("REAL_QID", String.valueOf(remotingCommand.getQueueId()));
            remotingCommand.setTopic("SCHEDULE_TOPIC_XXXX");
            remotingCommand.setQueueId(delayTimeLevel - 1);
        }

        this.brokerStartup.getMessageStore().appendMessage(remotingCommand.getTopic(), remotingCommand.getHey(), JSON.toJSONString(pMapper), remotingCommand.getQueueId());
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

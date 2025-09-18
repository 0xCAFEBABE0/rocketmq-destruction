package org.gnor.rocketmq.cretry_rNPC_14.broker.processor;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.cretry_rNPC_14.broker.BrokerStartup;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.ResponseCode;

import java.util.HashMap;
import java.util.Map;

public class SendMessageProcessor {

    private BrokerStartup brokerStartup;
    public SendMessageProcessor(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void processRequest(final Channel channel, RemotingCommand remotingCommand, RemotingCommand response) {
        String topic = remotingCommand.getTopic();
        //注册不存在的topic
        this.createTopicInSendMessageMethod(topic);

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

        this.brokerStartup.getMessageStore().appendMessage(remotingCommand.getTopic(), remotingCommand.getHey(), JSON.toJSONString(pMapper), remotingCommand.getQueueId(), 0);
        /*迭代为文件存储*/
        //List<RemotingCommand> storeList = storeTopicRecord.getOrDefault(topic, new ArrayList<>());
        //storeList.add(remotingCommand);
        //storeTopicRecord.put(topic, storeList);

        //storeMSG.add(remotingCommand); //存储消息
        response.setFlag(RemotingCommand.RESPONSE_FLAG);
        response.setCode(ResponseCode.SUCCESS);
        response.setHey("Response echo!!!!");
        channel.writeAndFlush(response);
        //requestHoldService.notifyMessageArriving(topic);
    }

    private void createTopicInSendMessageMethod(String topic) {
        RemotingCommand registerTopicCmd = new RemotingCommand();
        registerTopicCmd.setFlag(RemotingCommand.REQUEST_FLAG);
        registerTopicCmd.setCode(RemotingCommand.REGISTER_BROKER);
        registerTopicCmd.setBrokerName("Broker-01");
        registerTopicCmd.setBrokerAddr("127.0.0.1:9011");
        registerTopicCmd.setTopic(topic);
        registerTopicCmd.setTopicQueueNums(4);
        try {
            RemotingCommand responseCmd = brokerStartup.getRemotingClient().invokeSync("127.0.0.1:9091", registerTopicCmd, 30000L);
            System.out.println(responseCmd.getHey());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

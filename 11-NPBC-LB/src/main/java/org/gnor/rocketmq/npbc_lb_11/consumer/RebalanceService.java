package org.gnor.rocketmq.npbc_lb_11.consumer;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.npbc_lb_11.remoting.NettyRemotingClient;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RebalanceService implements Runnable {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();
    private PullMessageService pullMessageService;

    private final ConcurrentMap<String /*topic*/, List<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();

    public RebalanceService(PullMessageService pullMessageService) {
        this.pullMessageService = pullMessageService;
    }

    public void addTopicSubscribeInfo(String topic, MessageQueue mq) {
        this.topicSubscribeInfoTable.computeIfAbsent(topic, k -> new ArrayList<>()).add(mq);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            this.doRebalance();
            System.out.println("RebalanceService run");
        }
    }

    public boolean doRebalance() {
        List<Object> res = new ArrayList<>();
        topicSubscribeInfoTable.forEach((k, mqList) -> {
            List<String> cidAll = this.getConsumerIdList(k);
            Collections.sort(cidAll);
            //Collections.sort(mqList);
            String currentCID = buildMQClientId();

            int index = cidAll.indexOf(currentCID);  //当前消费者下标
            int mod = mqList.size() % cidAll.size();
            int averageSize = mqList.size() <= cidAll.size()
                    ? 1
                    : (mod > 0 && index < mod ? mqList.size() / cidAll.size() + 1 : mqList.size() / cidAll.size());
            int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
            int range = Math.min(averageSize, mqList.size() - startIndex);
            for (int i = 0; i< range; ++i) {
                res.add(mqList.get((startIndex + i) & mqList.size()));
            }
        });

        System.out.println("RebalanceService doRebalance " + JSONObject.toJSONString(res));
        return true;

    }

    public List<String> getConsumerIdList(String topic) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.GET_CONSUMER_LIST_BY_GROUP);
        remotingCommand.setTopic(topic);
        remotingCommand.setHey("Get consumer list by group");

        Map<String, String> brokerAddrTable = this.pullMessageService.brokerAddrTable;
        List<MessageQueue> mqs = topicSubscribeInfoTable.get(topic);
        String addr = brokerAddrTable.get(mqs.get(0).getBrokerName());
        try {
            RemotingCommand response = this.remotingClient.invokeSync(addr, remotingCommand, 3000L);
            return JSON.parseArray(response.getHey(), String.class);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append("127.0.0.1");

        sb.append("@");
        sb.append(this.instanceName);
        //if (!UtilAll.isBlank(this.unitName)) {
        //    sb.append("@");
        //    sb.append(this.unitName);
        //}
        return sb.toString();
    }
}

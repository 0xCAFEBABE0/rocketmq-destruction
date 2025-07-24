package org.gnor.rocketmq.npbc_namesrv_8.namesrv;

import org.gnor.rocketmq.common_1.TopicRouteData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NamesrvRequestProcessor {
    //存放broker对应地址
    private final Map<String/* brokerName */, String/* brokerAddr */> brokerAddrTable = new ConcurrentHashMap<>();
    //存放topic对应broker
    private final Map<String/* topic */, Map<String /* brokerName */, Integer /* queueNums */>> topicQueueTable = new ConcurrentHashMap<>();


    public void registerBroker(
            String brokerName,
            String brokerAddr,
            /*TODO@ch 需要独立接口*/
            String topic,
            int queueNums
    ) {
        this.brokerAddrTable.putIfAbsent(brokerName, brokerAddr);
        this.topicQueueTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).put(brokerName, queueNums);
    }

    public TopicRouteData getTopicRouteData(String topic) {
        Map<String, String> brokerAddrTable = new HashMap<>();
        Map<String, Integer> queueTable = new HashMap<>();
        for (String brokerName : this.topicQueueTable.getOrDefault(topic, new HashMap<>()).keySet()) {
            brokerAddrTable.put(brokerName, this.brokerAddrTable.get(brokerName));
            queueTable.put(brokerName, this.topicQueueTable.get(topic).get(brokerName));
        }
        return new TopicRouteData(topic, brokerAddrTable, queueTable);
    }
}

package org.gnor.rocketmq.npbc_namesrv_8.namesrv;

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
}

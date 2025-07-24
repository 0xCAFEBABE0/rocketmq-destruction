package org.gnor.rocketmq.common_1;

import java.util.Map;

public class TopicRouteData {
    private String topic;
    private Map<String /* brokerName */, String /* brokerAddr */> brokerAddrTable;
    private Map<String /* brokerName */, Integer /* queueNums */> queueTable;

    public TopicRouteData(String topic, Map<String, String> brokerAddrTable, Map<String, Integer> queueTable) {
        this.topic = topic;
        this.brokerAddrTable = brokerAddrTable;
        this.queueTable = queueTable;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public Map<String, String> getBrokerAddrTable() {
        return brokerAddrTable;
    }
    public void setBrokerAddrTable(Map<String, String> brokerAddrTable) {
        this.brokerAddrTable = brokerAddrTable;
    }
    public Map<String, Integer> getQueueTable() {
        return queueTable;
    }
    public void setQueueTable(Map<String, Integer> queueTable) {
        this.queueTable = queueTable;
    }
}

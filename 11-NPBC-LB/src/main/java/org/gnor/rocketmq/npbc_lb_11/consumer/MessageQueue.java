package org.gnor.rocketmq.npbc_lb_11.consumer;

public class MessageQueue {
    private String topic;
    private String brokerName;
    private int queueId;

    public MessageQueue() {
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public String getBrokerName() {
        return brokerName;
    }
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
    public int getQueueId() {
        return queueId;
    }
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}

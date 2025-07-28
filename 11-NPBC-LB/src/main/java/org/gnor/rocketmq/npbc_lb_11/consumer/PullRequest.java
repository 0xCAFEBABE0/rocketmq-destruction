package org.gnor.rocketmq.npbc_lb_11.consumer;

import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.TreeMap;

public class PullRequest {
    private String topic;
    private String brokerName;
    private int queueId;

    private TreeMap<Long /* queueOffset */, RemotingCommand> msgTreeMap = new TreeMap<>();
    private long nextOffset;

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

    public TreeMap<Long, RemotingCommand> getMsgTreeMap() {
        return msgTreeMap;
    }

    public void setMsgTreeMap(TreeMap<Long, RemotingCommand> msgTreeMap) {
        this.msgTreeMap = msgTreeMap;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }
}

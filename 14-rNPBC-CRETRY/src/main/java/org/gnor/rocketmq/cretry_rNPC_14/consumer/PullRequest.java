package org.gnor.rocketmq.cretry_rNPC_14.consumer;

import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.TreeMap;

public class PullRequest {
    private final MessageQueue messageQueue;
    private final ProcessQueue processQueue;
    private boolean dropped;
    private long nextOffset;

    public PullRequest(MessageQueue messageQueue, ProcessQueue processQueue) {
        this.messageQueue = messageQueue;
        this.processQueue = processQueue;
    }
    public MessageQueue getMessageQueue() {
        return messageQueue;
    }
    public ProcessQueue getProcessQueue() {
        return processQueue;
    }
    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }
    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }
}

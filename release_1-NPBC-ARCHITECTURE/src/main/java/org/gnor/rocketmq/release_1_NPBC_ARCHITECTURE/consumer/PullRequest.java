package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.consumer;

import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.TreeMap;

public class PullRequest {
    private final MessageQueue messageQueue;
    private final ProcessQueue processQueue;
    private boolean dropped;

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
}

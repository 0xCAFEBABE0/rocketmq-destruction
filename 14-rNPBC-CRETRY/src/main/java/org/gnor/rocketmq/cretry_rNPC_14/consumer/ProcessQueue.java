package org.gnor.rocketmq.cretry_rNPC_14.consumer;

import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.List;
import java.util.TreeMap;

public class ProcessQueue {
    private TreeMap<Long /* queueOffset */, RemotingCommand> msgTreeMap = new TreeMap<>();
    private volatile long queueOffsetMax = 0L;

    public boolean putMessage(List<RemotingCommand> msgs) {
        for (RemotingCommand msg : msgs) {
            RemotingCommand old = this.msgTreeMap.put(msg.getConsumerOffset(), msg);
            if (null == old) {
                this.queueOffsetMax = msg.getConsumerOffset();
            }
        }
        return true;
    }

    public long removeMessage(List<RemotingCommand> success) {
        long result = -1;
        if (msgTreeMap.isEmpty()) {
            return result;
        }
        result = this.queueOffsetMax + 1;
        int removedCnt = 0;
        for (RemotingCommand msg : success) {
            RemotingCommand prev = this.msgTreeMap.remove(msg.getConsumerOffset());
            if (prev != null) {
                --removedCnt;
            }
        }
        if (!msgTreeMap.isEmpty()) {
            result = this.msgTreeMap.firstKey();
        }
        return result;
    }

    public TreeMap<Long, RemotingCommand> getMsgTreeMap() {
        return msgTreeMap;
    }
}

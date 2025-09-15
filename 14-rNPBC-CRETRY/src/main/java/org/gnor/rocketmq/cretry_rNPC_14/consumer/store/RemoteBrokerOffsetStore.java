package org.gnor.rocketmq.cretry_rNPC_14.consumer.store;

import org.gnor.rocketmq.cretry_rNPC_14.consumer.DefaultMQPushConsumer;
import org.gnor.rocketmq.cretry_rNPC_14.consumer.MessageQueue;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.NettyRemotingClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RemoteBrokerOffsetStore {
    private ConcurrentMap<MessageQueue, ControllableOffset> offsetTable = new ConcurrentHashMap<>();

    private final DefaultMQPushConsumer consumer;
    public RemoteBrokerOffsetStore(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }

    public void updateOffset(MessageQueue mq, long offset) {
        if (null == mq) {
            return;
        }
        ControllableOffset old = this.offsetTable.putIfAbsent(mq, new ControllableOffset(offset));
        if (null == old) {
            old = this.offsetTable.putIfAbsent(mq, new ControllableOffset(offset));
        }
        if (null != old) {
            old.update(offset, true);
        }
    }

    public void persistAll(Set<MessageQueue> mqs) {
        for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ControllableOffset offset = entry.getValue();

            if (mqs.contains(mq)) {
                consumer.getPullMessageService().updateConsumeOffsetToBroker(mq, offset.getOffset());
            }
        }
    }

    public long readOffset(MessageQueue mq) {
        ControllableOffset offset = this.offsetTable.get(mq);
        if (null != offset) {
            return offset.getOffset();
        }
        return 0L;
    }
}

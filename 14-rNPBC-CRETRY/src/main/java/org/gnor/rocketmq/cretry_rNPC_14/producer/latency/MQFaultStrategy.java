package org.gnor.rocketmq.cretry_rNPC_14.producer.latency;

import org.gnor.rocketmq.common_1.ThreadLocalIndex;
import org.gnor.rocketmq.cretry_rNPC_14.producer.ProducerClient;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MQFaultStrategy {
    private volatile boolean sendLatencyFaultEnable = true;
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>(16);
    private ThreadLocalIndex sendQueue = new ThreadLocalIndex();

    private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};


    public interface QueueFilter {
        boolean filter(ProducerClient.MessageQueue mq);
    }

    private QueueFilter reachableFilter = mq -> isReachable(mq.getBrokerName());

    private QueueFilter availableFilter = mq -> isAvailable(mq.getBrokerName());

    public static class BrokerFilter implements QueueFilter {
        private String lastBrokerName;

        public void setLastBrokerName(String lastBrokerName) {
            this.lastBrokerName = lastBrokerName;
        }

        @Override
        public boolean filter(ProducerClient.MessageQueue mq) {
            if (lastBrokerName != null) {
                return !mq.getBrokerName().equals(lastBrokerName);
            }
            return true;
        }
    }

    public ProducerClient.MessageQueue selectOnMessageQueue(List<ProducerClient.MessageQueue> mqList, String lastBrokerName) {
        BrokerFilter brokerFilter = new BrokerFilter();
        brokerFilter.setLastBrokerName(lastBrokerName);
        if (this.sendLatencyFaultEnable) {
            ProducerClient.MessageQueue mq = filterOnMessageQueue(mqList, lastBrokerName, availableFilter, brokerFilter);
            if (null != mq) {
                return mq;
            }

            mq = filterOnMessageQueue(mqList, lastBrokerName, reachableFilter, brokerFilter);
            if (null != mq) {
                return mq;
            }
            return selectOneMessageQueue(mqList);
        }

        ProducerClient.MessageQueue mq = filterOnMessageQueue(mqList, lastBrokerName, brokerFilter);
        if (mq != null) {
            return mq;
        }
        return selectOneMessageQueue(mqList);
    }

    public ProducerClient.MessageQueue filterOnMessageQueue(List<ProducerClient.MessageQueue> mqList, String lastBrokerName, QueueFilter... filter) {
        ProducerClient.MessageQueue mq;
        if (null != filter && 0 != filter.length) {
            for (int i = 0; i < mqList.size(); ++i) {
                int index = Math.abs(this.sendQueue.incrementAndGet() % mqList.size());
                mq = mqList.get(index);

                boolean filterResult = true;
                for (QueueFilter f : filter) {
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }
            return null;
        }
        int index = Math.abs(this.sendQueue.incrementAndGet() % mqList.size());
        return mqList.get(index);
    }

    public ProducerClient.MessageQueue selectOneMessageQueue(List<ProducerClient.MessageQueue> mqList) {
        int index = this.sendQueue.incrementAndGet();
        int pos = index % mqList.size();
        return mqList.get(pos);
    }


    public boolean isAvailable(final String name) {
        FaultItem faultItem = this.faultItemTable.get(name);
        if (null != faultItem) {
            return faultItem.isAvailable();
        }
        return true;
    }

    public boolean isReachable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isReachable();
        }
        return true;
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, final boolean reachable) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(currentLatency);
            updateFaultItem(brokerName, currentLatency, duration, reachable);
        }
    }

    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration, final boolean reachable) {
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.updateNotAvailableDuration(notAvailableDuration);
            faultItem.setReachable(reachable);
            old = this.faultItemTable.putIfAbsent(name, faultItem);
        }

        if (null != old) {
            old.setCurrentLatency(currentLatency);
            old.updateNotAvailableDuration(notAvailableDuration);
            old.setReachable(reachable);
        }

        if (!reachable) {
            System.out.println(name + " is unreachable, it will not be used until it's reachable");
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return this.notAvailableDuration[i];
            }
        }

        return 0;
    }


    public class FaultItem implements Comparable<FaultItem> {
        private final String name;
        private volatile long currentLatency;
        private volatile long startTimestamp;
        private volatile long checkStamp;
        private volatile boolean reachableFlag;

        public FaultItem(final String name) {
            this.name = name;
        }

        public void updateNotAvailableDuration(long notAvailableDuration) {
            if (notAvailableDuration > 0 && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
                this.startTimestamp = notAvailableDuration + System.currentTimeMillis();
                System.out.println(name + " will be isolated for " + notAvailableDuration + " ms.");
            }
        }

        public boolean isAvailable() {
            return System.currentTimeMillis() >= this.startTimestamp;
        }

        public boolean isReachable() {
            return this.reachableFlag;
        }

        @Override
        public int compareTo(FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable()) {
                    return -1;
                }

                if (other.isAvailable()) {
                    return 1;
                }
            }

            if (this.currentLatency < other.currentLatency) {
                return -1;
            } else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp) {
                return -1;
            } else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "FaultItem{" +
                    "name='" + name + '\'' +
                    ", currentLatency=" + currentLatency +
                    ", startTimestamp=" + startTimestamp +
                    ", reachableFlag=" + reachableFlag +
                    '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public long getCheckStamp() {
            return this.checkStamp;
        }

        public void setReachable(boolean reachable) {
            this.reachableFlag = reachable;
        }
    }
}

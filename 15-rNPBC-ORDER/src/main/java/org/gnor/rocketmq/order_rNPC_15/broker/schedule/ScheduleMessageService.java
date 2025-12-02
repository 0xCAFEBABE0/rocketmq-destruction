package org.gnor.rocketmq.order_rNPC_15.broker.schedule;

import com.alibaba.fastjson2.JSON;
import org.gnor.rocketmq.common_1.ThreadFactoryImpl;
import org.gnor.rocketmq.order_rNPC_15.broker.BrokerStartup;
import org.gnor.rocketmq.order_rNPC_15.broker.store.ConsumeQueueStore;
import org.gnor.rocketmq.order_rNPC_15.broker.store.MessageStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScheduleMessageService {
    private final ConcurrentSkipListMap<Integer /*level*/, Long /*delay millis*/> delayLevelTable = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<>(32);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private int maxDelayLevel;
    String levelString = "1s 5s 10s 10s 10s 10s 10s 10s 10s 10s";

    public static final String RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";

    private ScheduledExecutorService deliverExecutorService;

    private BrokerStartup brokerStartup;

    public ScheduleMessageService(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            this.parseDelayLevel();

            this.deliverExecutorService = new ScheduledThreadPoolExecutor(maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"), new ThreadPoolExecutor.AbortPolicy());

            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);

                this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, null != offset ? offset: 0L), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void parseDelayLevel() {
        Map<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String[] levelArray = levelString.split(" ");
        for (int i = 0; i < levelArray.length; i++) {
            String value = levelArray[i];
            String ch = value.substring(value.length() - 1);
            Long tu = timeUnitTable.get(ch);

            int level = i + 1;
            if (level > this.maxDelayLevel) {
                this.maxDelayLevel = level;
            }
            long num = Long.parseLong(value.substring(0, value.length() - 1));
            long delayTimeMillis = tu * num;
            this.delayLevelTable.put(level, delayTimeMillis);
        }
    }

    class DeliverDelayedMessageTimerTask implements Runnable {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            this.executeOnTimeUp();
        }

        public void executeOnTimeUp() {
            ConsumeQueueStore consumeQueueStore = ScheduleMessageService.this.brokerStartup.getMessageStore().getConsumeQueueStore();
            ConsumeQueueStore.ConsumeQueue cq = consumeQueueStore.findOrCreateConsumeQueue(RMQ_SYS_SCHEDULE_TOPIC, this.delayLevel - 1);
            if (cq == null) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }

            MessageStore.MessageMetadata messageMetadata = cq.consumeMessage(this.offset);
            if (null == messageMetadata) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }
            long nextOffset = this.offset;

            int offsetPy = messageMetadata.getPosition();
            int sizePy = messageMetadata.getSize();
            long tagCode = messageMetadata.getTagCode();

            long now = System.currentTimeMillis();
            long deliverTimestamp = this.correctDeliverTimestamp(now, tagCode);
            nextOffset += 1;

            long countdown = deliverTimestamp - now;
            if (countdown > 0) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }
            MessageStore.StoredMessage storedMessage = brokerStartup.getMessageStore().getMessage(offsetPy, sizePy);

            System.out.println("Scheduler_time: storeMsg:" + JSON.toJSONString(storedMessage));
            storedMessage = messageTimeUp(storedMessage);
            brokerStartup.getMessageStore().appendMessage(storedMessage.getTopic(), storedMessage.getBody(), JSON.toJSONString(storedMessage.getProperties()), storedMessage.getQueueId(), storedMessage.getReconsumeTimes());
            offsetTable.put(delayLevel, nextOffset);

            this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
        }

        public void scheduleNextTimerTask(long offset, long delay) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, offset), delay, TimeUnit.MILLISECONDS);
        }

        private long correctDeliverTimestamp(long now, long deliverTimestamp) {
            long result = deliverTimestamp;
            long maxTimestamp = now + delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }
            return result;
        }
    }

    private MessageStore.StoredMessage messageTimeUp(MessageStore.StoredMessage msg) {
        MessageStore.StoredMessage storedMessaged = new MessageStore.StoredMessage(msg.getTopic(), msg.getBody(), "FOUND", msg.getQueueId(), msg.getProperties(), msg.getReconsumeTimes(), msg.getCommitLogOffset());

        Map<String, String> properties = storedMessaged.getProperties();
        storedMessaged.setTopic(properties.get("REAL_TOPIC"));
        int queueId = Integer.parseInt(properties.get("REAL_QID"));
        storedMessaged.setQueueId(queueId);
        return storedMessaged;
    }
}

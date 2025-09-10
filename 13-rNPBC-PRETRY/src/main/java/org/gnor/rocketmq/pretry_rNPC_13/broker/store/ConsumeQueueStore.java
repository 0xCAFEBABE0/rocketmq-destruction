package org.gnor.rocketmq.pretry_rNPC_13.broker.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class ConsumeQueueStore {
    protected final ConcurrentMap<String /*topic*/, Map<Integer /*queueId*/, ConsumeQueue>> consumeQueueTable = new ConcurrentHashMap<>();

    public boolean hasMessages(String topic, long pullFromThisOffset, int queueId) {
        ConsumeQueue consumeQueue = consumeQueueTable.get(topic).get(queueId);
        return null != consumeQueue && pullFromThisOffset < consumeQueue.wrotePosition;
    }

    public MessageStore.MessageMetadata consumeMessage(String topic, long pullFromThisOffset, int queueId) {
        ConsumeQueue consumeQueue = consumeQueueTable.get(topic).get(queueId);
        if (null == consumeQueue) {
            return null;
        }
        return consumeQueue.consumeMessage(pullFromThisOffset);
    }

    /**
     * ConsumeQueue's store unit. Format:
     * <pre>
     * ┌───────────────────────────────┬───────────────────┬───────────────────────────────┐
     * │    CommitLog Physical Offset  │      Body Size    │            Tag HashCode       │
     * │          (8 Bytes)            │      (4 Bytes)    │             (8 Bytes)         │
     * ├───────────────────────────────┴───────────────────┴───────────────────────────────┤
     * │                                     Store Unit                                    │
     * │                                                                                   │
     * </pre>
     * ConsumeQueue's store unit. Size: CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) = 20 Bytes
     */
    public static class ConsumeQueue {
        private ByteBuffer writeBuffer;
        protected FileChannel fileChannel;
        protected MappedByteBuffer mappedByteBuffer;

        private final String topic;
        //v9版本新增 queueId
        private final int queueId;

        protected volatile int wrotePosition;
        //protected volatile int readPosition;
        protected static final AtomicIntegerFieldUpdater<ConsumeQueue> WROTE_POSITION_UPDATER;
        //protected static final AtomicIntegerFieldUpdater<ConsumeQueue> READ_POSITION_UPDATER;

        static {
            WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ConsumeQueue.class, "wrotePosition");
            //READ_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ConsumeQueue.class, "readPosition");
        }


        private int mappedFileSizeCQ = 10 * 1024 * 1024;
        //v7版本新增 tagCode
        //private int cqSize = 12;
        private static final int CQ_STORE_UNIT_SIZE = 20;

        public ConsumeQueue(String topic, int queueId) {
            this.topic = topic;
            this.queueId = queueId;
            this.writeBuffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB buffer

            File cqFile = new File("/Users/qudian/data/store/consumequeue/" + topic + "/" + queueId + "/00000000000000000000");
            try {
                File f = new File(cqFile.getParent());
                if (!f.exists()) {
                    boolean result = f.mkdirs();
                }
                this.fileChannel = new RandomAccessFile(cqFile, "rw").getChannel();
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedFileSizeCQ);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /*release_1-NPBC-ARCHITECTURE */
        //对外offset皆为index
        public long convertIndexToCqOffset(long index) {
            return index * ConsumeQueue.CQ_STORE_UNIT_SIZE;
        }

        public void appendMessage(int size, long commitLogPos, long tagCode) {
            writeBuffer.clear();
            writeBuffer.putLong(commitLogPos);
            writeBuffer.putInt(size);
            writeBuffer.putLong(tagCode);
            writeBuffer.flip();

            int currentPos = WROTE_POSITION_UPDATER.get(this);
            mappedByteBuffer.position(currentPos);
            mappedByteBuffer.put(writeBuffer);
            WROTE_POSITION_UPDATER.addAndGet(this, CQ_STORE_UNIT_SIZE);
            this.mappedByteBuffer.force();
        }

        public MessageStore.MessageMetadata getCommitLogMetaFromCq(int cqPos) {
            ByteBuffer readBuffer = mappedByteBuffer.duplicate();
            readBuffer.position(cqPos);
            readBuffer.limit(cqPos + CQ_STORE_UNIT_SIZE);
            long physicOffset = readBuffer.getLong();
            int size = readBuffer.getInt();
            long tagCode = readBuffer.getLong();
            return new MessageStore.MessageMetadata((int) physicOffset, size, topic, cqPos + CQ_STORE_UNIT_SIZE, tagCode);
        }

        public MessageStore.MessageMetadata consumeMessage(long pullFromThisOffset) {
            int currentPos = (int) convertIndexToCqOffset(pullFromThisOffset);
            if (currentPos >= wrotePosition) {
                return null;
            }
            MessageStore.MessageMetadata metadata = getCommitLogMetaFromCq(currentPos);
            //READ_POSITION_UPDATER.addAndGet(this, cqSize);
            return metadata;
        }

    }

    public ConsumeQueue findOrCreateConsumeQueue(String topic, int queueId) {
        return consumeQueueTable.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).computeIfAbsent(queueId, k -> new ConsumeQueue(topic, queueId));
    }

    public void appendMessage(String topic, int size, long offset, long tagCode, int queueId) {
        ConsumeQueue consumeQueue = findOrCreateConsumeQueue(topic, queueId);
        consumeQueue.appendMessage(size, offset, tagCode);
    }

}



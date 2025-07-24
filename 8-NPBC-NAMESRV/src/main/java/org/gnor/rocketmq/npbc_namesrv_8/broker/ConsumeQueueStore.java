package org.gnor.rocketmq.npbc_namesrv_8.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class ConsumeQueueStore {
    protected final ConcurrentMap<String /*topic*/, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();

    public boolean hasMessages(String topic, long pullFromThisOffset) {
        ConsumeQueue consumeQueue = consumeQueueTable.get(topic);
        return null != consumeQueue && pullFromThisOffset < consumeQueue.wrotePosition;
    }

    public MessageStore.MessageMetadata consumeMessage(String topic, long pullFromThisOffset) {
        ConsumeQueue consumeQueue = consumeQueueTable.get(topic);
        if (null == consumeQueue) {
            return null;
        }
        return consumeQueue.consumeMessage(pullFromThisOffset);
    }

    public static class ConsumeQueue {
        private ByteBuffer writeBuffer;
        protected FileChannel fileChannel;
        protected MappedByteBuffer mappedByteBuffer;

        private final String topic;

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
        private int cqSize = 20;

        public ConsumeQueue(String topic) {
            this.topic = topic;
            this.writeBuffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB buffer

            File cqFile = new File("/Users/qudian/data/store/consumequeue/" + topic + "/00000000000000000000");
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

        public void appendMessage(int size, long offset, long tagCode) {
            writeBuffer.clear();
            writeBuffer.putLong(offset);
            writeBuffer.putInt(size);
            writeBuffer.putLong(tagCode);
            writeBuffer.flip();

            int currentPos = WROTE_POSITION_UPDATER.get(this);
            mappedByteBuffer.position(currentPos);
            mappedByteBuffer.put(writeBuffer);
            WROTE_POSITION_UPDATER.addAndGet(this, cqSize);
            this.mappedByteBuffer.force();
        }

        public MessageStore.MessageMetadata getCommitLogMetaFromCq(int cqPos) {
            ByteBuffer readBuffer = mappedByteBuffer.duplicate();
            readBuffer.position(cqPos);
            readBuffer.limit(cqPos + cqSize);
            long physicOffset = readBuffer.getLong();
            int size = readBuffer.getInt();
            long tagCode = readBuffer.getLong();
            return new MessageStore.MessageMetadata((int) physicOffset, size, topic, cqPos + cqSize, tagCode);
        }

        public MessageStore.MessageMetadata consumeMessage(long pullFromThisOffset) {
            int currentPos = (int)pullFromThisOffset;
            if (currentPos >= wrotePosition) {
                return null;
            }
            MessageStore.MessageMetadata metadata = getCommitLogMetaFromCq(currentPos);
            //READ_POSITION_UPDATER.addAndGet(this, cqSize);
            return metadata;
        }

    }

    public ConsumeQueue findOrCreateConsumeQueue(String topic) {
        return consumeQueueTable.computeIfAbsent(topic, k -> new ConsumeQueue(topic));
    }

    public void appendMessage(String topic, int size, long offset, long tagCode) {
        ConsumeQueue consumeQueue = findOrCreateConsumeQueue(topic);
        consumeQueue.appendMessage(size, offset, tagCode);
    }

}

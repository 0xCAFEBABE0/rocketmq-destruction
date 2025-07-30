package org.gnor.rocketmq.pbc_offset_6.broker;

import com.sun.xml.internal.ws.api.message.MessageMetadata;
import org.gnor.rocketmq.pbc_offset_6.broker.ConsumeQueueStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class MessageStore {
    private ByteBuffer writeBuffer;

    protected FileChannel fileChannel;
    protected MappedByteBuffer mappedByteBuffer;

    protected volatile int wrotePosition;
    protected static final AtomicIntegerFieldUpdater<MessageStore> WROTE_POSITION_UPDATER;

    /*v5版本新增：consumeQueue*/
    private ConsumeQueueStore consumeQueueStore;

    // CommitLog file size,default is 10M
    //private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;
    private int mappedFileSizeCommitLog = 10 * 1024 * 1024;

    //TEMP-> Track message metadata for each topic
    //private final ConcurrentMap<String /*topic*/, List<MessageMetadata>> topicMessageIndex = new ConcurrentHashMap<>();
    /*v6 */
    private ConsumerOffsetManager consumerOffsetManager;

    // Message metadata class to track position and size
    public static class MessageMetadata {
        private final int position;
        private final int size;
        private final String topic;

        //v6版本新增
        private long cqPosition;

        public MessageMetadata(int position, int size, String topic, long cqPosition) {
            this.position = position;
            this.size = size;
            this.topic = topic;
            this.cqPosition = cqPosition;
        }

        public int getPosition() { return position; }
        public int getSize() { return size; }
        public String getTopic() { return topic; }
        public long getCqPosition() { return cqPosition; }
    }

    public static class StoredMessage {
        private final String topic;
        private final String body;

        public StoredMessage(String topic, String body) {
            this.topic = topic;
            this.body = body;
        }

        public String getTopic() { return topic; }
        public String getBody() { return body; }
    }

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessageStore.class, "wrotePosition");
    }

    public MessageStore(ConsumerOffsetManager consumerOffsetManager) {
        this.consumerOffsetManager = consumerOffsetManager;
        this.writeBuffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB buffer

        File commitLog = new File("/Users/qudian/data/store/commitlog/00000000000000000000");
        try {
            File f = new File(commitLog.getParent());
            if (!f.exists()) {
                boolean result = f.mkdirs();
            }
            this.fileChannel = new RandomAccessFile(commitLog, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedFileSizeCommitLog);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.consumeQueueStore = new ConsumeQueueStore();
    }

    public int calMsgLength(int bodyLength, int topicLength) {
        return 4 //TOTALSIZE
                + 8 //PHYSICALOFFSET
                + 4 + (Math.max(bodyLength, 0)) //BODY
                + 2 + topicLength //TOPIC
                ;
    }

    /**
     * Check if there are any messages available for a topic
     */
    public boolean hasMessages(String topic, long pullFromThisOffset) {
        //List<MessageStore.MessageMetadata> metadataList = topicMessageIndex.get(topic);
        //return null != metadataList && !metadataList.isEmpty();
        //v5版本新增：consumeQueue
        return consumeQueueStore.hasMessages(topic, pullFromThisOffset);
    }

    public void encode(String topic, String body) {
        byte[] topicData = topic.getBytes(StandardCharsets.UTF_8);
        int topicLength = topicData.length;

        byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);
        int bodyLength = bodyData.length;

        int msgLength = calMsgLength(bodyLength, topicLength);

        writeBuffer.clear(); // Clear the buffer before writing
        writeBuffer.putInt(msgLength);
        writeBuffer.putLong(0); //PHYSICALOFFSET, need update later
        writeBuffer.putInt(bodyLength);
        writeBuffer.put(bodyData);
        writeBuffer.putShort((short)topicLength);
        writeBuffer.put(topicData);
        writeBuffer.flip(); // Prepare for reading
        System.out.println("msgLength: " + msgLength);
    }

    public void appendMessage(String topic, String body) {
        encode(topic, body);

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        // Position the mapped buffer to the current write position
        mappedByteBuffer.position(currentPos);
        // Write the encoded message to the mapped buffer
        mappedByteBuffer.put(writeBuffer);
        // Update the write position
        int msgLength = writeBuffer.getInt(0);
        WROTE_POSITION_UPDATER.addAndGet(this, msgLength);

        //TEMP->consume record
        //MessageMetadata metadata = new MessageMetadata(currentPos, msgLength, topic);
        //topicMessageIndex.computeIfAbsent(topic, k -> new ArrayList<>()).add(metadata);

        this.mappedByteBuffer.force();
        System.out.println("Appended message at position: " + currentPos + ", length: " + msgLength);

        //v5版本新增：consumeQueue
        consumeQueueStore.appendMessage(topic, msgLength, currentPos);
    }

    public StoredMessage getMessage(int pos, int size) {
        // Create a duplicate to avoid affecting the original buffer's position
        ByteBuffer readBuffer = mappedByteBuffer.duplicate();
        readBuffer.position(pos);
        readBuffer.limit(pos + size);

        int storeSize = readBuffer.getInt();
        long physicOffset = readBuffer.getLong();
        int bodySize = readBuffer.getInt();
        byte[] bodyData = new byte[bodySize];
        readBuffer.get(bodyData);
        int topicSize = readBuffer.getShort() & 0xFFFF; // Convert to unsigned
        byte[] topicData = new byte[topicSize];
        readBuffer.get(topicData);

        System.out.println("Reading from position: " + pos + ", size: " + size);
        System.out.println("storeSize: " + storeSize);
        System.out.println("physicOffset: " + physicOffset);
        System.out.println("bodySize: " + bodySize);
        System.out.println("body: " + new String(bodyData, StandardCharsets.UTF_8));
        System.out.println("topicSize: " + topicSize);
        System.out.println("topic: " + new String(topicData, StandardCharsets.UTF_8));
        System.out.println("---");

        return new StoredMessage(new String(topicData), new String(bodyData));
    }

    /**
     * Get and remove the first message for a specific topic (consume)
     */
    //public StoredMessage consumeMessage(String topic) {
    //    List<MessageMetadata> metadataList = topicMessageIndex.get(topic);
    //    if (null == metadataList || metadataList.isEmpty()) {
    //        return null;
    //    }
    //    MessageMetadata metadata = metadataList.remove(0);
    //    // If no more messages for this topic, remove the topic entry
    //    if (metadataList.isEmpty()) {
    //        topicMessageIndex.remove(topic);
    //    }
    //    return getMessage(metadata.getPosition(), metadata.getSize());
    //}

    public StoredMessage consumeMessage(String topic, long pullFromThisOffset) {
        MessageMetadata metadata = consumeQueueStore.consumeMessage(topic, pullFromThisOffset);
        if (null == metadata) {
            return null;
        }
        //v6版本新增：commitOffsetManager，写回commitOffset
        this.consumerOffsetManager.commitOffset(topic, metadata.getCqPosition());
        return getMessage(metadata.getPosition(), metadata.getSize());
    }

    public static void main(String[] args) {
        MessageStore messageStore = new MessageStore(new ConsumerOffsetManager());
        messageStore.appendMessage("Topic-test", "aaaHello.");
        messageStore.appendMessage("Topic-test", "2aaaHello.");

        messageStore.getMessage(0, 37);
        messageStore.getMessage( 37, 38);
    }
}

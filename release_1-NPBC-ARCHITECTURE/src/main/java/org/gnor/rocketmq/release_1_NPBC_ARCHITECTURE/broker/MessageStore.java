package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker;


import com.alibaba.fastjson2.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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

    private ConsumerOffsetManager consumerOffsetManager;

    // Message metadata class to track position and size
    public static class MessageMetadata {
        private final int position;
        private final int size;
        private final String topic;
        private long tagCode;

        //v6版本新增
        private long cqPosition;

        public MessageMetadata(int position, int size, String topic, long cqPosition, long tagCode) {
            this.position = position;
            this.size = size;
            this.topic = topic;
            this.cqPosition = cqPosition;
            this.tagCode = tagCode;
        }

        public int getPosition() { return position; }
        public int getSize() { return size; }
        public String getTopic() { return topic; }
        public long getCqPosition() { return cqPosition; }
        public long getTagCode() { return tagCode; }
    }

    public static class StoredMessage {
        private final String topic;
        private final String body;
        private final String status;
        private final int queueId;

        public StoredMessage(String topic, String body, String status, int queueId) {
            this.topic = topic;
            this.body = body;
            this.status = status;
            this.queueId = queueId;
        }

        public String getTopic() { return topic; }
        public String getBody() { return body; }
        public String getStatus() { return status; }
        public int getQueueId() { return queueId; }
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

    public int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        return 4 //TOTALSIZE
                + 8 //PHYSICALOFFSET
                + 4 //QUEUEID
                + 4 + (Math.max(bodyLength, 0)) //BODY
                + 2 + topicLength //TOPIC
                + 2 + (Math.max(propertiesLength, 0)) //PROPERTIES
                ;
    }

    /**
     * Check if there are any messages available for a topic
     */
    public boolean hasMessages(String topic, long pullFromThisOffset, int queueId) {
        //List<MessageStore.MessageMetadata> metadataList = topicMessageIndex.get(topic);
        //return null != metadataList && !metadataList.isEmpty();
        //v5版本新增：consumeQueue
        return consumeQueueStore.hasMessages(topic, pullFromThisOffset, queueId);
    }

    public void encode(String topic, String body, String properties, int queueId) {
        byte[] topicData = topic.getBytes(StandardCharsets.UTF_8);
        int topicLength = topicData.length;

        byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);
        int bodyLength = bodyData.length;

        int propertiesLength = properties.length();
        byte[] propertiesBytes = properties.getBytes(StandardCharsets.UTF_8);

        int msgLength = calMsgLength(bodyLength, topicLength, propertiesLength);

        writeBuffer.clear(); // Clear the buffer before writing
        writeBuffer.putInt(msgLength);
        writeBuffer.putLong(0); //PHYSICALOFFSET, need update later
        writeBuffer.putInt(queueId); //QUEUEID, need update later
        writeBuffer.putInt(bodyLength);
        writeBuffer.put(bodyData);
        writeBuffer.putShort((short)topicLength);
        writeBuffer.put(topicData);
        writeBuffer.putShort((short)propertiesLength);
        writeBuffer.put(propertiesBytes);
        writeBuffer.flip(); // Prepare for reading
        System.out.println("msgLength: " + msgLength);
    }

    public void appendMessage(String topic, String body, String properties, int queueId) {
        encode(topic, body, properties, queueId);

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
        //v7版本新增：tag
        Map<String, String> propMap = JSONObject.parseObject(properties, Map.class);
        String tag = propMap.get("TAG");
        int tagCode = tag.hashCode();
        consumeQueueStore.appendMessage(topic, msgLength, currentPos, tagCode, queueId);
    }

    public StoredMessage getMessage(int pos, int size) {
        // Create a duplicate to avoid affecting the original buffer's position
        ByteBuffer readBuffer = mappedByteBuffer.duplicate();
        readBuffer.position(pos);
        readBuffer.limit(pos + size);

        int storeSize = readBuffer.getInt();
        long physicOffset = readBuffer.getLong();
        int queueId = readBuffer.getInt();
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

        return new StoredMessage(new String(topicData), new String(bodyData), "FOUND", queueId);
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

    public StoredMessage consumeMessage(String topic, long pullFromThisOffset, String tag, int queueId) {
        MessageMetadata metadata = consumeQueueStore.consumeMessage(topic, pullFromThisOffset, queueId);
        if (null == metadata) {
            return null;
        }

        //v6版本新增：commitOffsetManager，写回commitOffset
        this.consumerOffsetManager.commitOffset(topic, metadata.getCqPosition(), queueId);
        //v7版本新增：tagCode
        if (null != tag && !tag.isEmpty()) {
            //NO_MATCHED_MESSAGE
            if (metadata.getTagCode() != tag.hashCode()) {
                return new StoredMessage(topic, "", "NO_MATCHED_MESSAGE", queueId);
            }
        }
        return getMessage(metadata.getPosition(), metadata.getSize());
    }

    public static void main(String[] args) {
        MessageStore messageStore = new MessageStore(new ConsumerOffsetManager());
        messageStore.appendMessage("Topic-test", "aaaHello.", "", 0);
        messageStore.appendMessage("Topic-test", "2aaaHello.", "", 0);

        messageStore.getMessage(0, 37);
        messageStore.getMessage( 37, 38);
    }
}

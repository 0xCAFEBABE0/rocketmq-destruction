package org.gnor.rocketmq.pbc_file_4.broker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class MessageStore {
    private ByteBuf byteBuf;

    protected FileChannel fileChannel;
    protected MappedByteBuffer mappedByteBuffer;

    protected volatile int wrotePosition;
    protected static final AtomicIntegerFieldUpdater<MessageStore> WROTE_POSITION_UPDATER;

    // CommitLog file size,default is 10M
    //private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;
    private int mappedFileSizeCommitLog = 10 * 1024 * 1024;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessageStore.class, "wrotePosition");
    }

    public MessageStore() {
        ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;
        this.byteBuf = alloc.directBuffer(Integer.MAX_VALUE);

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
    }

    public int calMsgLength(int bodyLength, int topicLength) {
        return 4 //TOTALSIZE
                + 8 //PHYSICALOFFSET
                + 4 + (Math.max(bodyLength, 0)) //BODY
                + 2 + topicLength //TOPIC
                ;
    }

    public void encode(String topic, String body) {
        byte[] topicData = topic.getBytes(StandardCharsets.UTF_8);
        int topicLength = topicData.length;

        byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);
        int bodyLength = bodyData.length;

        int msgLength = calMsgLength(bodyLength, topicLength);

        byteBuf.writeInt(msgLength);
        byteBuf.writeLong(0); //PHYSICALOFFSET, need update later
        byteBuf.writeInt(bodyLength);
        byteBuf.writeBytes(bodyData);
        byteBuf.writeShort(topicLength);
        byteBuf.writeBytes(topicData);
    }

    public void appendMessage(String topic, String body) {
        encode(topic, body);
        mappedByteBuffer.put(byteBuf.nioBuffer());

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        ByteBuffer slice = this.mappedByteBuffer.slice();
        slice.position(currentPos);

        WROTE_POSITION_UPDATER.addAndGet(this, byteBuf.getInt(0));
        this.mappedByteBuffer.force();
    }

    public void getMessage(int pos, int size) {
        ByteBuffer slice = mappedByteBuffer.slice();
        slice.position(pos);
        ByteBuffer newSlice = slice.slice();
        newSlice.limit(size);

        int storeSize = newSlice.getInt();

    }

    public static void main(String[] args) {
        MessageStore messageStore = new MessageStore();
        messageStore.appendMessage("Topic-test", "aaaHello.");


    }
}

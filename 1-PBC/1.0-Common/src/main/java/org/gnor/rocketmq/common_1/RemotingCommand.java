package org.gnor.rocketmq.common_1;

import com.alibaba.fastjson2.JSON;
import io.netty.buffer.ByteBuf;

/**
 * @version 1.0
 * @since 2025/7/2
 */
public class RemotingCommand {
    private int flag = REQUEST_FLAG;
    private int code = PRODUCER_MSG;
    private String hey;
    private transient byte[] body;

    /*v3版本新增 */
    private String topic;

    /*v6版本新增 */
    private long consumerOffset;
    private String consumerGroup;

    public static final int REQUEST_FLAG = 0;
    public static final int RESPONSE_FLAG = 1;


    public static final int PRODUCER_MSG = 101;
    public static final int CONSUMER_MSG = 102;

    public static final int QUERY_CONSUMER_OFFSET = 201;

    public String getHey() {
        return hey;
    }

    public void setHey(String hey) {
        this.hey = hey;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    private static RemotingCommand headerDecode(ByteBuf byteBuffer, int len, int type) {
        switch (type) {
            case 0:  //JSON
                byte[] headerData = new byte[len];
                byteBuffer.readBytes(headerData);
                RemotingCommand resultJson = JSON.parseObject(headerData, RemotingCommand.class);
                return resultJson;
            //case 1:  //ROCKETMQ
            //    RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(byteBuffer, len);
            //    return resultRMQ;
            default:
                break;
        }

        return null;
    }

    public static RemotingCommand decode(final ByteBuf byteBuffer) {
        int length = byteBuffer.readableBytes();
        int oriHeaderLen = byteBuffer.readInt();
        int headerLength = getHeaderLength(oriHeaderLen);
        //if (headerLength > length - 4) {
        //    throw new RemotingCommandException("decode error, bad header length: " + headerLength);
        //}

        RemotingCommand cmd = headerDecode(byteBuffer, headerLength, (oriHeaderLen >> 24) & 0xFF);

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.readBytes(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }
}

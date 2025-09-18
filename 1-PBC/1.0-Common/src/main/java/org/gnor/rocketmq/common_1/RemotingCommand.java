package org.gnor.rocketmq.common_1;

import com.alibaba.fastjson2.JSON;
import io.netty.buffer.ByteBuf;
import org.gnor.rocketmq.common_1.annotation.CFNotNull;
import org.gnor.rocketmq.common_1.protocol.header.CommandCustomHeader;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version 1.0
 * @since 2025/7/2
 */
public class RemotingCommand {
    private transient CommandCustomHeader customHeader;
    private transient CommandCustomHeader cachedHeader;
    private Map<String, String> extFields;
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<>();
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();


    private int flag = REQUEST_FLAG;
    private int code = PRODUCER_MSG;
    private String hey;
    private transient byte[] body;
    private String properties;

    /*v3版本新增 */
    private String topic;
    /*v9版本新增 */
    private int queueId;

    /*v6版本新增 */
    private long consumerOffset;
    private String consumerGroup;

    /*v8版本新增 namesrv*/
    private String brokerName;
    private String brokerAddr;
    private int topicQueueNums;
    private TopicRouteData topicRouteData;

    /*v11版本新增*/
    private String clientId;

    /*release_1版本新增*/
    private static final AtomicInteger requestId = new AtomicInteger(0);
    private int opaque = requestId.getAndIncrement();
    private Long commitOffset;
    private Long nextBeginOffset;

    public static final int REQUEST_FLAG = 0;
    public static final int RESPONSE_FLAG = 1;


    public static final int PRODUCER_MSG = 101;
    public static final int CONSUMER_MSG = 102;

    public static final int QUERY_CONSUMER_OFFSET = 201;

    public static final int GET_ROUTEINFO_BY_TOPIC = 105;
    public static final int REGISTER_BROKER = 103;
    public static final int UNREGISTER_BROKER = 104;

    public static final int BROKER_HEARTBEAT = 904;

    /*v11版本新增*/
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;
    public static final int HEART_BEAT = 34;

    /*release_1版本新增*/
    public static final int UPDATE_CONSUMER_OFFSET = 15;

    /*v12版本新增*/
    public static final int TOPIC_NOT_EXIST = 17;

    /*v14版本新增*/
    public static final int CONSUMER_SEND_MSG_BACK = 36;


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public void setTopicQueueNums(int topicQueueNums) {
        this.topicQueueNums = topicQueueNums;
    }

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

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public int getTopicQueueNums() {
        return topicQueueNums;
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public void setCustomHeader(CommandCustomHeader header) {
        this.customHeader = header;
    }

    public Map<String, String> getExtFields() {
        return this.extFields;
    }
    public void setExtFields(Map<String, String> extFields) {
        this.extFields = extFields;
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

    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }

            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                String name = field.getName();
                if (name.startsWith("this")) {
                    continue;
                }
                Object value = null;
                try {
                    field.setAccessible(true);
                    value = field.get(this.customHeader);
                } catch (Exception e) {
                    System.out.println("makeCustomHeaderToNet Exception");
                }

                if (value != null) {
                    this.extFields.put(name, value.toString());
                }
            }
        }
    }

    //make it able to test
    Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);
        if (null != field) {
            return field;
        }

        Set<Field> fieldList = new HashSet<>();
        for (Class className = classHeader; className != Object.class; className = className.getSuperclass()) {
            Field[] fields = className.getDeclaredFields();
            fieldList.addAll(Arrays.asList(fields));
        }
        field = fieldList.toArray(new Field[0]);
        synchronized (CLASS_HASH_MAP) {
            CLASS_HASH_MAP.put(classHeader, field);
        }
        return field;
    }


    public <T extends CommandCustomHeader> T decodeCommandCustomHeader(Class<T> classHeader) {
        return decodeCommandCustomHeader(classHeader, false);
    }


    public <T extends CommandCustomHeader> T decodeCommandCustomHeader(Class<T> classHeader, boolean isCached) {
        if (isCached && cachedHeader != null) {
            return classHeader.cast(cachedHeader);
        }
        cachedHeader = decodeCommandCustomHeaderDirectly(classHeader, true);
        if (cachedHeader == null) {
            return null;
        }
        return classHeader.cast(cachedHeader);
    }

    public <T extends CommandCustomHeader> T decodeCommandCustomHeaderDirectly(Class<T> classHeader, boolean useFastEncode) {
        T objectHeader;
        try {
            objectHeader = classHeader.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
        if (null == this.extFields) {
            return objectHeader;
        }

        Field[] fields = getClazzFields(classHeader);
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String fieldName = field.getName();
                if (!fieldName.startsWith("this")) {
                    try {
                        String value = this.extFields.get(fieldName);
                        if (null == value) {
                            if (!isFieldNullable(field)) {
                                throw new RuntimeException("the custom field <" + fieldName + "> is null");
                            }
                            continue;
                        }

                        field.setAccessible(true);
                        String type = getCanonicalName(field.getType());
                        Object valueParsed;

                        if (type.equals(STRING_CANONICAL_NAME)) {
                            valueParsed = value;
                        } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                            valueParsed = Integer.parseInt(value);
                        } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                            valueParsed = Long.parseLong(value);
                        } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                            valueParsed = Boolean.parseBoolean(value);
                        } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                            valueParsed = Double.parseDouble(value);
                        } else {
                            throw new RuntimeException("the custom field <" + fieldName + "> type is not supported");
                        }

                        field.set(objectHeader, valueParsed);

                    } catch (Throwable e) {
                        System.out.println("Failed field [{" + fieldName + "}] decoding");
                    }
                }
            }
        }

        objectHeader.checkFields();

        return objectHeader;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }
}

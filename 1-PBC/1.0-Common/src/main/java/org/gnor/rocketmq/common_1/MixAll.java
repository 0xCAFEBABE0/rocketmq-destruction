package org.gnor.rocketmq.common_1;

public class MixAll {

    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    public static String getRetryTopic(final String consumerGroup) {
        return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
    }
}

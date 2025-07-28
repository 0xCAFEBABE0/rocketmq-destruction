package org.gnor.rocketmq.npbc_lb_11.broker.client;

import io.netty.channel.Channel;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ConsumerManager {
    private final ConcurrentMap<String /*clientId*/, ConsumerInfo> consumerTable = new ConcurrentHashMap<>();

    public boolean registerConsumer(String clientId, Channel channel, String topic, Set<String> tagsCode) {
        this.consumerTable.computeIfAbsent(clientId, k -> new ConsumerInfo(clientId, channel, topic, tagsCode));
        return true;
    }

    public List<String> getConsumerListByTopic(String topic) {
        return this.consumerTable.values().stream()
                .filter(consumerInfo -> null != consumerInfo.getSubscription(topic))
                .map(ConsumerInfo::getAllClientId)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}

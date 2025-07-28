package org.gnor.rocketmq.npbc_lb_11.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ConsumerInfo {
    private final ConcurrentMap<String /*topic*/, Set<String> /*tagsCode*/> subscriptionTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, String /*clientId*/> channelTable = new ConcurrentHashMap<>();

    public ConsumerInfo(String clientId, Channel channel, String topic, Set<String> tagsCode) {
        this.channelTable.put(channel, clientId);
        this.subscriptionTable.put(topic, tagsCode);
    }

    public void addSubscription(String topic, Set<String> tagsCode) {
        this.subscriptionTable.put(topic, tagsCode);
    }
    public Set<String> getSubscription(String topic) {
        return this.subscriptionTable.get(topic);
    }

    public String getClientId(Channel channel) {
        return this.channelTable.get(channel);
    }

    public List<String> getAllClientId() {
        return new ArrayList<>(this.channelTable.values());
    }
}

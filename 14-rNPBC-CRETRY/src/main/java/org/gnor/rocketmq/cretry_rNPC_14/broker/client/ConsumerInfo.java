package org.gnor.rocketmq.cretry_rNPC_14.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ConsumerInfo {
    private final ConcurrentMap<String /*topic*/, Set<String> /*tagsCode*/> subscriptionTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, ClientChannelInfo> channelTable = new ConcurrentHashMap<>();

    public ConsumerInfo(String clientId, Channel channel, String topic, Set<String> tagsCode) {
        this.channelTable.put(channel, new ClientChannelInfo(channel, clientId));
        this.subscriptionTable.put(topic, tagsCode);
    }

    public void addSubscription(String topic, Set<String> tagsCode) {
        this.subscriptionTable.put(topic, tagsCode);
    }
    public Set<String> getSubscription(String topic) {
        return this.subscriptionTable.get(topic);
    }

    public String getClientId(Channel channel) {
        return this.channelTable.get(channel).getClientId();
    }

    public List<String> getAllClientId() {
        return this.channelTable.values().stream().map(ClientChannelInfo::getClientId).collect(Collectors.toList());
    }

    public void setLastUpdateTimestamp(Channel channel) {
        this.channelTable.get(channel).setLastUpdateTimestamp(System.currentTimeMillis());
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelTable() {
        return this.channelTable;
    }
}

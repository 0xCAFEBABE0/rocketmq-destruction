package org.gnor.rocketmq.npbc_lb_11.broker.client;

import io.netty.channel.Channel;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ConsumerManager {
    private final ConcurrentMap<String /*clientId*/, ConsumerInfo> consumerTable = new ConcurrentHashMap<>();

    public boolean registerConsumer(String clientId, Channel channel, String topic, Set<String> tagsCode) {
        ConsumerInfo old = this.consumerTable.get(clientId);
        if (null == old) {
            this.consumerTable.put(clientId, new ConsumerInfo(clientId, channel, topic, tagsCode));
        } else {
            old.setLastUpdateTimestamp(channel);
        }
        return true;
    }

    public List<String> getConsumerListByTopic(String topic) {
        return this.consumerTable.values().stream()
                .filter(consumerInfo -> null != consumerInfo.getSubscription(topic))
                .map(ConsumerInfo::getAllClientId)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void scanNotActivateChannel() {
        Iterator<Map.Entry<String, ConsumerInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConsumerInfo> next = it.next();
            ConsumerInfo consumerInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerInfo.getChannelTable();
            Iterator<Map.Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Map.Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > 30_000) {
                    //clientChannelInfo.getChannel().close();
                    itChannel.remove();
                }
            }
            if (channelInfoTable.isEmpty()) {
                System.out.println("SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup=" + next.getKey());
                it.remove();
            }
        }
    }
}

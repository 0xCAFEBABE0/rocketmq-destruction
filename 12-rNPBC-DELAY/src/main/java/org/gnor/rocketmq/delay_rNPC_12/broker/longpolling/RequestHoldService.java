package org.gnor.rocketmq.delay_rNPC_12.broker.longpolling;

import com.alibaba.fastjson2.JSONObject;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.thread.ServiceThread;
import org.gnor.rocketmq.delay_rNPC_12.broker.BrokerStartup;
import org.gnor.rocketmq.delay_rNPC_12.broker.store.MessageStore;
import org.gnor.rocketmq.delay_rNPC_12.broker.store.SuspendRequest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RequestHoldService extends ServiceThread {
    protected ConcurrentMap<String /*topic@queueId*/ , List<SuspendRequest>> suspendRequests = new ConcurrentHashMap<>();

    public ConcurrentMap<String /*topic@queueId*/ , List<SuspendRequest>> getSuspendRequests() {
        return suspendRequests;
    }

    /*v3版本新增：读取本地存储*/
    protected final BrokerStartup brokerStartup;
    public RequestHoldService(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    @Override
    public void run() {
        System.out.println(this.getServiceName() + " started.");
        while (!this.stopped) {
            try {
                this.waitForRunning(5 * 1000);

                long startTime = System.currentTimeMillis();
                this.checkHoldRequest();
                long costTime = System.currentTimeMillis() - startTime;
                if (costTime > 5000) {
                    System.out.println("Long Polling cost too much time. " + costTime + "ms");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void checkHoldRequest() throws InterruptedException {
        notifyMessageArriving();
    }

    public void notifyMessageArriving() throws InterruptedException {
        for (String k : this.suspendRequests.keySet()) {
            this.notifyMessageArriving(k);
        }
    }

    public void notifyMessageArriving(String key) throws InterruptedException {
        List<SuspendRequest> suspendRequests = this.suspendRequests.get(key);
        if (null == suspendRequests || suspendRequests.isEmpty()) {
            System.out.println("没有请求需要唤醒。");
            return;
        }
        String[] ks = key.split(SuspendRequest.TOPIC_QUEUEID_SEPARATOR);
        String topic = ks[0];

        //ConcurrentMap<String, List<RemotingCommand>> storeTopicRecord = this.brokerStartup.getStoreTopicRecord();
        //List<RemotingCommand> storeDataList = storeTopicRecord.get(topic);
        MessageStore messageStore = brokerStartup.getMessageStore();

        Iterator<SuspendRequest> it = suspendRequests.iterator();
        while (it.hasNext()) {
            SuspendRequest sr = it.next();

            int queueId = sr.getQueueId();
            boolean hasMessages = messageStore.hasMessages(topic, sr.getPullFromThisOffset(), queueId);
            if (!hasMessages && System.currentTimeMillis() >= sr.getSuspendTimestamp() + 15000L) {
                RemotingCommand msgNotFound = new RemotingCommand();
                msgNotFound.setHey("Message not found!");
                msgNotFound.setFlag(RemotingCommand.RESPONSE_FLAG);
                msgNotFound.setOpaque(sr.getOpaque());
                sr.getClientChannel().writeAndFlush(msgNotFound);
                it.remove();
            } else if (hasMessages) {

                RemotingCommand requestCommand = sr.getRequestCommand();
                Map<String, String> properties = JSONObject.parseObject(requestCommand.getProperties(), HashMap.class);
                String tag = properties.get("TAG");

                MessageStore.StoredMessage storedMessage = messageStore.consumeMessage(topic, sr.getPullFromThisOffset(), tag, queueId);
                if (null == storedMessage) {
                    continue;
                }
                RemotingCommand msgArrivingCmd = new RemotingCommand();
                msgArrivingCmd.setCode( RemotingCommand.CONSUMER_MSG);
                msgArrivingCmd.setOpaque(sr.getOpaque());
                //TODO@ch
                msgArrivingCmd.setNextBeginOffset(sr.getPullFromThisOffset() + 1);
                msgArrivingCmd.setConsumerOffset(sr.getPullFromThisOffset());
                if ("NO_MATCHED_MESSAGE".equals(storedMessage.getStatus())) {
                    msgArrivingCmd.setHey("NO_MATCHED_MESSAGE");
                    msgArrivingCmd.setTopic(storedMessage.getTopic());
                    msgArrivingCmd.setFlag(RemotingCommand.RESPONSE_FLAG);
                } else {
                    msgArrivingCmd.setHey(storedMessage.getBody());
                    msgArrivingCmd.setTopic(storedMessage.getTopic());
                    msgArrivingCmd.setFlag(RemotingCommand.RESPONSE_FLAG);
                }
                sr.getClientChannel().writeAndFlush(msgArrivingCmd).sync();
                it.remove();
            }
        }
    }

    @Override
    public String getServiceName() {
        return RequestHoldService.class.getSimpleName();
    }
}

package org.gnor.rocketmq.pbc_file_4.broker;

import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RequestHoldService implements Runnable {
    protected ConcurrentMap<String /*topic*/ , List<SuspendRequest>> suspendRequests = new ConcurrentHashMap<>();

    public ConcurrentMap<String /*topic*/ , List<SuspendRequest>> getSuspendRequests() {
        return suspendRequests;
    }

    /*v3版本新增：读取本地存储*/
    protected final BrokerStartup brokerStartup;
    public RequestHoldService(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000L);
                checkHoldRequest();
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

    public void notifyMessageArriving(String topic) throws InterruptedException {
        List<SuspendRequest> suspendRequests = this.suspendRequests.get(topic);
        if (null == suspendRequests || suspendRequests.isEmpty()) {
            System.out.println("没有请求需要唤醒。");
            return;
        }
        ConcurrentMap<String, List<RemotingCommand>> storeTopicRecord = this.brokerStartup.getStoreTopicRecord();
        List<RemotingCommand> storeDataList = storeTopicRecord.get(topic);

        Iterator<SuspendRequest> it = suspendRequests.iterator();
        while (it.hasNext()) {
            SuspendRequest sr = it.next();

            if (storeDataList.isEmpty() && System.currentTimeMillis() >= sr.getSuspendTimestamp() + 15000L) {
                RemotingCommand msgNotFound = new RemotingCommand();
                msgNotFound.setHey("Message not found!");
                msgNotFound.setFlag(RemotingCommand.RESPONSE_FLAG);
                sr.getClientChannel().writeAndFlush(msgNotFound);
                it.remove();
            } else if (!storeDataList.isEmpty()) {
                Iterator<RemotingCommand> storeIt = storeDataList.iterator();
                while (storeIt.hasNext()) {
                    RemotingCommand msgArrivingCmd = storeIt.next();
                    msgArrivingCmd.setFlag(RemotingCommand.RESPONSE_FLAG);
                    sr.getClientChannel().writeAndFlush(msgArrivingCmd).sync();
                    storeIt.remove();
                }
                it.remove();
            }
        }
    }
}

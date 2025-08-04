package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.consumer;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.remoting.NettyRemotingClient;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RebalanceService implements Runnable {
    private NettyRemotingClient remotingClient = new NettyRemotingClient();
    private PullMessageService pullMessageService;

    private final ConcurrentMap<String /*topic*/, List<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();

    protected final ConcurrentMap<MessageQueue, PullRequest> processQueueTable = new ConcurrentHashMap<>(64);

    public RebalanceService(PullMessageService pullMessageService) {
        this.pullMessageService = pullMessageService;
    }

    public void addTopicSubscribeInfo(String topic, MessageQueue mq) {
        this.topicSubscribeInfoTable.computeIfAbsent(topic, k -> new ArrayList<>()).add(mq);
    }

    public ConcurrentMap<MessageQueue, PullRequest> getProcessQueueTable() {
        return this.processQueueTable;
    }


    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            this.doRebalance();
            System.out.println("RebalanceService run");
        }
    }

    public boolean doRebalance() {
        List<MessageQueue> allocateResult = new ArrayList<>();
        topicSubscribeInfoTable.forEach((topic, mqList) -> {
            List<String> cidAll = this.getConsumerIdList(topic);
            Collections.sort(cidAll);
            //Collections.sort(mqList);
            String currentCID = buildMQClientId();

            int index = cidAll.indexOf(currentCID);  //当前消费者下标
            int mod = mqList.size() % cidAll.size();
            int averageSize = mqList.size() <= cidAll.size()
                    ? 1
                    : (mod > 0 && index < mod ? mqList.size() / cidAll.size() + 1 : mqList.size() / cidAll.size());
            int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
            int range = Math.min(averageSize, mqList.size() - startIndex);
            for (int i = 0; i< range; ++i) {
                allocateResult.add(mqList.get(startIndex + i));
            }

            System.out.println("RebalanceService doRebalance " + JSONObject.toJSONString(allocateResult));

            // drop process queues no longer belong me
            HashMap<MessageQueue, PullRequest> removeQueueMap = new HashMap<>(this.processQueueTable.size());
            Iterator<Map.Entry<MessageQueue, PullRequest>> it = this.processQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, PullRequest> next = it.next();
                MessageQueue mq = next.getKey();
                PullRequest pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!allocateResult.contains(mq)) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Map.Entry<MessageQueue, PullRequest> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                PullRequest pq = entry.getValue();
                //if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                //    this.processQueueTable.remove(mq);
                //}
                this.processQueueTable.remove(mq);
            }

            // add new message queue
            boolean allMQLocked = true;
            List<PullRequest> pullRequestList = new ArrayList<>();
            for (MessageQueue mq : mqList) {
                if (!this.processQueueTable.containsKey(mq) && !removeQueueMap.containsKey(mq)) {
                    //if (needLockMq && !this.lock(mq)) {
                    //    System.out.println("doRebalance, add a new mq failed, {}, because lock failed" + mq);
                    //    allMQLocked = false;
                    //    continue;
                    //}
                    //
                    //this.removeDirtyOffset(mq);
                    //ProcessQueue pq = createProcessQueue();
                    //pq.setLocked(true);
                    //long nextOffset = this.computePullFromWhere(mq);
                    //if (nextOffset >= 0) {
                    PullRequest pq = new PullRequest(mq, new ProcessQueue());
                    PullRequest pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        System.out.println("doRebalance, add a new mq failed, {}, because mq already exists" + mq);
                    } else {
                        System.out.println("doRebalance, add a new mq, " + mq);

                        MessageQueue messageQueue = new MessageQueue(mq.getTopic(), mq.getBrokerName(), mq.getQueueId());
                        PullRequest pullRequest = new PullRequest(messageQueue, new ProcessQueue());
                        pullRequestList.add(pullRequest);
                    }
                    //} else {
                    //    System.out.println("doRebalance, add new mq failed, {}" + mq);
                    //}
                }
            }
            pullRequestList.forEach(pullMessageService::executePullRequest);
        });
        return true;
    }

    public List<String> getConsumerIdList(String topic) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.GET_CONSUMER_LIST_BY_GROUP);
        remotingCommand.setTopic(topic);
        remotingCommand.setHey("Get consumer list by group");

        Map<String, String> brokerAddrTable = this.pullMessageService.brokerAddrTable;
        List<MessageQueue> mqs = topicSubscribeInfoTable.get(topic);
        String addr = brokerAddrTable.get(mqs.get(0).getBrokerName());
        try {
            RemotingCommand response = this.remotingClient.invokeSync(addr, remotingCommand, 3000L);
            return JSON.parseArray(response.getHey(), String.class);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append("127.0.0.1");

        sb.append("@");
        sb.append(this.instanceName);
        //if (!UtilAll.isBlank(this.unitName)) {
        //    sb.append("@");
        //    sb.append(this.unitName);
        //}
        return sb.toString();
    }
}

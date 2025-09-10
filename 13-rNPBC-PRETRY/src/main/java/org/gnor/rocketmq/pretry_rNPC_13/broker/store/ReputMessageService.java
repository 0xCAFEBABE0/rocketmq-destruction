package org.gnor.rocketmq.pretry_rNPC_13.broker.store;

import org.gnor.rocketmq.common_1.thread.ServiceThread;
import org.gnor.rocketmq.pretry_rNPC_13.broker.BrokerStartup;

import java.util.concurrent.TimeUnit;

public class ReputMessageService extends ServiceThread {
    //commitLog位点
    protected volatile long reputFromOffset = 0;
    private BrokerStartup brokerStartup;

    public ReputMessageService(BrokerStartup brokerStartup) {
        this.brokerStartup = brokerStartup;
    }

    @Override
    public void run() {
        while (!this.stopped) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                this.doReput();
            } catch (Throwable e) {
                e.printStackTrace();
                System.out.println(this.getServiceName() + " service has exception. " + e);
            }
        }
    }

    @Override
    public String getServiceName() {
        return ReputMessageService.class.getSimpleName();
    }

    public boolean isCommitLogAvailable() {
        return this.reputFromOffset < this.brokerStartup.getMessageStore().getReadPosition();
    }

    private void doReput() throws InterruptedException {
        boolean isCommitLogAvailable = isCommitLogAvailable();
        for (boolean doNext = true; isCommitLogAvailable && doNext; ) {

            MessageStore.SelectMappedBufferResult result = this.brokerStartup.getMessageStore().getData((int) this.reputFromOffset);
            if (result == null) {
                break;
            }
            this.reputFromOffset = result.getStartOffset();
            for (int readSize = 0; readSize < result.getSize() && isCommitLogAvailable() && doNext; ) {

                MessageStore.DispatchRequest dispatchRequest = this.brokerStartup.getMessageStore().checkMessageAndReturnSize(result.getByteBuffer());
                int size = dispatchRequest.getMsgSize();
                if (reputFromOffset + size > this.brokerStartup.getMessageStore().getReadPosition()) {
                    doNext = false;
                    break;
                }
                //doDispatch
                this.brokerStartup.getMessageStore().putMessageToCq(dispatchRequest);
                //激活长轮询等待请求
                String key = SuspendRequest.buildKey(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
                this.brokerStartup.getRequestHoldService().notifyMessageArriving(key);

                this.reputFromOffset += size;
                readSize += size;
            }
        }

    }
}

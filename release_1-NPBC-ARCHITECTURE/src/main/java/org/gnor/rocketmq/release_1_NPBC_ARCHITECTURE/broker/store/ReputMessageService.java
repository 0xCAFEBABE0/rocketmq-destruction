package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.broker.store;

import org.gnor.rocketmq.common_1.thread.ServiceThread;

public class ReputMessageService extends ServiceThread {
    protected volatile long reputFromOffset = 0;

    @Override
    public void run() {

    }

    @Override
    public String getServiceName() {
        return ReputMessageService.class.getSimpleName();
    }
}

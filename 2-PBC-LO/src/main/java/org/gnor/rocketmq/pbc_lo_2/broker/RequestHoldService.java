package org.gnor.rocketmq.pbc_lo_2.broker;

import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class RequestHoldService implements Runnable {
    protected List<SuspendRequest> suspendRequestList = new ArrayList<>();

    public List<SuspendRequest> getSuspendRequestList() {
        return suspendRequestList;
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

    private void checkHoldRequest() {
        notifyMessageArriving(null);
    }

    public void notifyMessageArriving(RemotingCommand arrivingCommand) {
        List<SuspendRequest> suspendRequests = this.suspendRequestList;
        if (suspendRequests != null) {

            Iterator<SuspendRequest> it = suspendRequests.iterator();
            while (it.hasNext()) {
                SuspendRequest sr = it.next();
                if (Objects.isNull(arrivingCommand) && System.currentTimeMillis() >= sr.getSuspendTimestamp() + 15000L) {
                    RemotingCommand msgNotFound = new RemotingCommand();
                    msgNotFound.setHey("Message not found!");
                    msgNotFound.setFlag(RemotingCommand.RESPONSE_FLAG);
                    sr.getClientChannel().writeAndFlush(msgNotFound);
                    it.remove();
                } else if (!Objects.isNull(arrivingCommand)) {
                    arrivingCommand.setFlag(RemotingCommand.RESPONSE_FLAG);
                    sr.getClientChannel().writeAndFlush(arrivingCommand);
                    it.remove();
                }
            }
        } else {
            System.out.println("没有请求需要唤醒。");
        }
    }
}

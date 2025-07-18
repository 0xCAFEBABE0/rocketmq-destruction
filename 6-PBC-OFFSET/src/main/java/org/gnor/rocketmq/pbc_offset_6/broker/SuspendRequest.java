package org.gnor.rocketmq.pbc_offset_6.broker;

import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;

/**
 * @version 1.0
 * @since 2025/7/4
 */
public class SuspendRequest {
    private Channel clientChannel;
    private RemotingCommand requestCommand;
    //v6版本新增
    private final long pullFromThisOffset;

    private long suspendTimestamp;

    public SuspendRequest(Channel clientChannel, RemotingCommand requestCommand, long suspendTimestamp, long pullFromThisOffset) {
        this.clientChannel = clientChannel;
        this.requestCommand = requestCommand;
        this.suspendTimestamp = suspendTimestamp;
        this.pullFromThisOffset = pullFromThisOffset;
    }

    public Channel getClientChannel() {
        return clientChannel;
    }

    public void setClientChannel(Channel clientChannel) {
        this.clientChannel = clientChannel;
    }

    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }

    public void setRequestCommand(RemotingCommand requestCommand) {
        this.requestCommand = requestCommand;
    }

    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }

    public void setSuspendTimestamp(long suspendTimestamp) {
        this.suspendTimestamp = suspendTimestamp;
    }

    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }
}

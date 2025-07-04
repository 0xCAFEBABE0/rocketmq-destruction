package org.gnor.rocketmq.pbc_lo_2.broker;

import io.netty.channel.Channel;
import org.gnor.rocketmq.common_1.RemotingCommand;

/**
 * @version 1.0
 * @since 2025/7/4
 */
public class SuspendRequest {
    private Channel clientChannel;
    private RemotingCommand requestCommand;

    private long suspendTimestamp;

    public SuspendRequest(Channel clientChannel, RemotingCommand requestCommand, long suspendTimestamp) {
        this.clientChannel = clientChannel;
        this.requestCommand = requestCommand;
        this.suspendTimestamp = suspendTimestamp;
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
}

package org.gnor.rocketmq.broker_1;

import io.netty.bootstrap.ServerBootstrap;

/**
 * @version 1.0
 * @since 2025/7/1
 */
public class BrokerStartup {
    private final ServerBootstrap serverBootstrap;

    public BrokerStartup() {
        this.serverBootstrap = new ServerBootstrap();;
    }

    protected void initServerBootstrap(ServerBootstrap serverBootstrap) {
        serverBootstrap.group();
    }

}

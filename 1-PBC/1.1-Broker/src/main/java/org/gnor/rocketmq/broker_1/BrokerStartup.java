package org.gnor.rocketmq.broker_1;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.gnor.rocketmq.common_1.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 * @since 2025/7/1
 */
public class BrokerStartup {
    private final ServerBootstrap serverBootstrap;
    protected final EventLoopGroup eventLoopGroupSelector;
    protected final EventLoopGroup eventLoopGroupBoss;
    protected final NettyServerHandler serverHandler;

    //本地存储列表
    private static List<RemotingCommand> storeMSG = new ArrayList<>();

    public BrokerStartup() {
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupSelector = new NioEventLoopGroup(3, new ThreadFactoryImpl("NettyServerNIOSelector_"));
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        this.serverHandler = new NettyServerHandler();
    }

    public void start() {
        initServerBootstrap(serverBootstrap);
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            System.out.println("Broker started, listening 0.0.0.0:" + addr.getPort());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new BrokerStartup().start();
    }

    protected void initServerBootstrap(ServerBootstrap serverBootstrap) {
        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(Constant.BROKER_IP, Constant.BROKER_PORT))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new DefaultEventExecutorGroup(8, new ThreadFactoryImpl("NettyServerCodecThread_")),
                                        new NettyEncoder(),
                                        new NettyDecoder(),
                                        new IdleStateHandler(0, 0, 120),
                                        serverHandler);
                    }
                });
    }

    @ChannelHandler.Sharable
    public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) {
            System.out.println("Received MSG:" + remotingCommand.getHey());
            if (RemotingCommand.REQUEST_FLAG == remotingCommand.getFlag()) {

                RemotingCommand response = new RemotingCommand();
                switch (remotingCommand.getCode()) {
                    case RemotingCommand.PRODUCER_MSG:
                        storeMSG.add(remotingCommand); //存储消息

                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setHey("Response echo!!!!");
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    case RemotingCommand.CONSUMER_MSG:
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        String hey = "Message`s empty!";
                        if (!storeMSG.isEmpty()) {
                            hey = storeMSG.get(0).getHey();
                            storeMSG.remove(0);
                        }
                        response.setHey(hey);
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    default:
                        break;
                }
            }

        }
    }

}

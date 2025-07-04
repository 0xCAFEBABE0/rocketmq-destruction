package org.gnor.rocketmq.pbc_topic_3.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.gnor.rocketmq.common_1.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @version 1.0
 * @since 2025/7/1
 */
public class BrokerStartup {
    private final ServerBootstrap serverBootstrap;
    protected final EventLoopGroup eventLoopGroupSelector;
    protected final EventLoopGroup eventLoopGroupBoss;
    protected final NettyServerHandler serverHandler;

    /*v2版本新增：消费长轮询机制*/
    protected final RequestHoldService requestHoldService;

    protected final NettyConnectManageHandler connectionManageHandler = new NettyConnectManageHandler();

    /*v3版本新增：本地按topic存储列表*/
    private final ConcurrentMap<String /*topic*/, List<RemotingCommand>> storeTopicRecord = new ConcurrentHashMap<>();
    public ConcurrentMap<String /*topic*/, List<RemotingCommand>> getStoreTopicRecord() {
        return storeTopicRecord;
    }

    public BrokerStartup() {
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupSelector = new NioEventLoopGroup(3, new ThreadFactoryImpl("NettyServerNIOSelector_"));
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        this.serverHandler = new NettyServerHandler();

        this.requestHoldService = new RequestHoldService(this);
    }

    public void start() {
        initServerBootstrap(serverBootstrap);
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            System.out.println("Broker started, listening 0.0.0.0:" + addr.getPort());

            new Thread(requestHoldService).start();
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
                                        connectionManageHandler,
                                        serverHandler);
                    }
                });
    }

    @ChannelHandler.Sharable
    public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) throws InterruptedException {
            System.out.println("Received MSG:" + remotingCommand.getHey());
            if (RemotingCommand.REQUEST_FLAG == remotingCommand.getFlag()) {

                String topic = remotingCommand.getTopic();
                RemotingCommand response = new RemotingCommand();
                switch (remotingCommand.getCode()) {
                    case RemotingCommand.PRODUCER_MSG:

                        List<RemotingCommand> storeList = storeTopicRecord.getOrDefault(topic, new ArrayList<>());
                        storeList.add(remotingCommand);
                        storeTopicRecord.put(topic, storeList);
                        //storeMSG.add(remotingCommand); //存储消息
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setHey("Response echo!!!!");
                        channelHandlerContext.channel().writeAndFlush(response);

                        requestHoldService.notifyMessageArriving(topic);
                        break;
                    case RemotingCommand.CONSUMER_MSG:
                        SuspendRequest sr = new SuspendRequest(channelHandlerContext.channel(), remotingCommand, System.currentTimeMillis());
                        //todo 可移到RequestHoldService统一管理
                        ConcurrentMap<String, List<SuspendRequest>> suspendRequests = requestHoldService.getSuspendRequests();
                        List<SuspendRequest> suspendRequestList = suspendRequests.getOrDefault(topic, new ArrayList<>());
                        suspendRequestList.add(sr);
                        suspendRequests.put(topic, suspendRequestList);
                        break;
                    default:
                        break;
                }
            }

        }
    }

    @ChannelHandler.Sharable
    public class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            System.out.println("NETTY SERVER PIPELINE: channelRegistered");
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            System.out.println("NETTY SERVER PIPELINE: channelUnregistered");
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("NETTY SERVER PIPELINE: channelActive");
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("NETTY SERVER PIPELINE: channelInactive");
            super.channelInactive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    System.out.println("NETTY SERVER PIPELINE: IDLE exception");
                    ctx.channel().close();
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.out.println("NETTY SERVER PIPELINE: exceptionCaught exception." + cause);
            ctx.channel().close();
        }
    }

}

package org.gnor.rocketmq.npbc_lb_11.namesrv;

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

public class NamesrvStartup {
    private final ServerBootstrap serverBootstrap;
    protected final EventLoopGroup eventLoopGroupSelector;
    protected final EventLoopGroup eventLoopGroupBoss;
    protected final NettyConnectManageHandler connectionManageHandler = new NettyConnectManageHandler();
    protected final NamesrvNettyServerHandler serverHandler;
    protected final NamesrvRequestProcessor namesrvRequestProcessor = new NamesrvRequestProcessor();

    public NamesrvStartup() {
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupSelector = new NioEventLoopGroup(3, new ThreadFactoryImpl("Namesrv_NettyServerNIOSelector_"));
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("Namesrv_NettyNIOBoss_"));
        this.serverHandler = new NamesrvNettyServerHandler();
    }

    public void start() {
        initServerBootstrap(serverBootstrap);
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            System.out.println("Namesrv started, listening 0.0.0.0:" + addr.getPort());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new NamesrvStartup().start();
    }

    protected void initServerBootstrap(ServerBootstrap serverBootstrap) {
        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(Constant.BROKER_IP, Constant.NAMESRV_PORT))
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
    public class NamesrvNettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) throws InterruptedException {
            System.out.println("Namesrv:Received MSG:" + remotingCommand.getHey());
            if (RemotingCommand.REQUEST_FLAG == remotingCommand.getFlag()) {

                String topic = remotingCommand.getTopic();
                RemotingCommand response = new RemotingCommand();
                switch (remotingCommand.getCode()) {
                    case RemotingCommand.REGISTER_BROKER:
                        namesrvRequestProcessor.registerBroker(
                                remotingCommand.getBrokerName(),
                                remotingCommand.getBrokerAddr(),
                                remotingCommand.getTopic(),
                                remotingCommand.getTopicQueueNums()
                        );
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setHey("Namesrv:Broker registered!");
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    case RemotingCommand.GET_ROUTEINFO_BY_TOPIC:
                        TopicRouteData topicRouteData = namesrvRequestProcessor.getTopicRouteData(topic);
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setTopicRouteData(topicRouteData);
                        channelHandlerContext.channel().writeAndFlush(response);
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

package org.gnor.rocketmq.pbc_file_4.consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gnor.rocketmq.common_1.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ConsumerClient_02 {
    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerKeepingService"));
    private Channel channel = null;
    private volatile boolean isRunning = false;

    public static void main(String[] args) throws Exception {
        new ConsumerClient_02().run();
    }

    public void run() throws Exception {
        String host = Constant.BROKER_IP;
        int port = Constant.BROKER_PORT;
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                    .channel(NioSocketChannel.class) // 使用NIO传输Channel
                    .option(ChannelOption.SO_KEEPALIVE, false) // 保持连接
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(
                                            new NettyEncoder(),
                                            new NettyDecoder()
                                    )
                                    .addLast(new ClientHandler());
                        }
                    });

            // 连接服务器
            ChannelFuture f = b.connect(host, port).sync();
            this.channel = f.channel();
            this.isRunning = true;
            System.out.println("已连接到服务器: " + host + ":" + port);
            //startScheduledPullMessage();
            pullMessage();
            // 等待连接关闭
            f.channel().closeFuture().sync();
        } finally {
            this.isRunning = false;
            if (scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
                scheduledExecutorService.shutdown();
            }
            workerGroup.shutdownGracefully();
        }
    }

    public void pullMessage() {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.CONSUMER_MSG);
        remotingCommand.setTopic("Topic-T02");
        remotingCommand.setHey("Pull message request from consumer");

        System.out.println("发送拉取消息请求: " + remotingCommand.getHey());
        channel.writeAndFlush(remotingCommand);
    }

    // 客户端处理器
    public class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            // 读取服务器发送的消息
            if (msg.getFlag() == RemotingCommand.RESPONSE_FLAG) {
                System.out.println("收到服务器响应消息: " + msg.getHey() + " [时间: " +
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]");

                ConsumerClient_02.this.pullMessage();
            } else {
                System.out.println("收到服务器消息: " + msg.getHey());
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("连接已激活，开始定时拉取消息...");
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("连接已断开");
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 异常处理
            System.err.println("连接异常: " + cause.getMessage());
            ctx.close();
        }
    }

}

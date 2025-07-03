package org.gnor.rocketmq.producer_1;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gnor.rocketmq.common_1.Constant;
import org.gnor.rocketmq.common_1.NettyDecoder;
import org.gnor.rocketmq.common_1.NettyEncoder;
import org.gnor.rocketmq.common_1.RemotingCommand;

public class ProducerClient {

    public static void main(String[] args) throws Exception {
        new ProducerClient().run();
    }

    public void run() throws Exception {
        // 配置客户端
        String host = Constant.BROKER_IP;
        int port = Constant.BROKER_PORT;
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                    .channel(NioSocketChannel.class) // 使用NIO传输Channel
                    .option(ChannelOption.SO_KEEPALIVE, true) // 保持连接
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
            System.out.println("已连接到服务器: " + host + ":" + port);

            // 发送消息
            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            remotingCommand.setCode(RemotingCommand.PRODUCER_MSG);
            remotingCommand.setHey("Hello, Producer Server!");
            f.channel().writeAndFlush(remotingCommand);
            System.out.println("已发送消息: " + remotingCommand.getHey());

            // 等待连接关闭
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    // 客户端处理器
    static class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            // 读取服务器发送的消息
            System.out.println("收到服务器消息: " + msg.getHey());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 异常处理
            cause.printStackTrace();
            ctx.close();
        }
    }

}

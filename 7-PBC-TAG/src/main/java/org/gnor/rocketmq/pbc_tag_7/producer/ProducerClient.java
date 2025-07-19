package org.gnor.rocketmq.pbc_tag_7.producer;

import com.alibaba.fastjson2.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gnor.rocketmq.common_1.Constant;
import org.gnor.rocketmq.common_1.NettyDecoder;
import org.gnor.rocketmq.common_1.NettyEncoder;
import org.gnor.rocketmq.common_1.RemotingCommand;

import java.util.HashMap;

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

            ChannelFuture f = b.connect(host, port).sync();
            System.out.println("已连接到服务器: " + host + ":" + port);

            // 发送消息
            RemotingCommand remotingCommand = new RemotingCommand();
            remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
            remotingCommand.setCode(RemotingCommand.PRODUCER_MSG);
            remotingCommand.setTopic("Topic-T01");
            remotingCommand.setHey("Hello, Producer-01 Server!");
            remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
                put("TAG", "TAG-A");
            }}));
            f.channel().writeAndFlush(remotingCommand).sync();
            System.out.println("已发送消息: " + remotingCommand.getHey());

            // 发送消息
            remotingCommand.setTopic("Topic-T02");
            remotingCommand.setHey("Hello, Producer-02 Server!");
            remotingCommand.setProperties(JSON.toJSONString(new HashMap<String, String >() {{
                put("TAG", "TAG-A");
            }}));
            f.channel().writeAndFlush(remotingCommand);
            System.out.println("已发送消息: " + remotingCommand.getHey());

            // 等待连接关闭
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    static class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            System.out.println("收到服务器消息: " + msg.getHey());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

}

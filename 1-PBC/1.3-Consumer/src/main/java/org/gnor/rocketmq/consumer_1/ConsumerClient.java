package org.gnor.rocketmq.consumer_1;

import com.alibaba.fastjson2.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.gnor.rocketmq.common_1.RemotingCommand;

import java.nio.charset.StandardCharsets;

public class ConsumerClient {
    private final String host;
    private final int port;

    public ConsumerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run() throws Exception {
        // 配置客户端
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
            remotingCommand.setCode(RemotingCommand.CONSUMER_MSG);
            remotingCommand.setHey("Hello, Consumer Server!");
            f.channel().writeAndFlush(remotingCommand);
            System.out.println("已发送消息: " + remotingCommand.getHey());

            // 等待连接关闭
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 9111;
        new ConsumerClient(host, port).run();
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

    public static class NettyDecoder extends LengthFieldBasedFrameDecoder {
        private static final int FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", Integer.MAX_VALUE + ""));

        public NettyDecoder() {
            super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
        }

        @Override
        public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = null;
            try {
                frame = (ByteBuf) super.decode(ctx, in);
                if (null == frame) {
                    return null;
                }
                RemotingCommand cmd = RemotingCommand.decode(frame);
                return cmd;
            } catch (Exception e) {
                //RemotingHelper.closeChannel(ctx.channel());
            } finally {
                if (null != frame) {
                    frame.release();
                }
            }

            return null;
        }
    }

    @ChannelHandler.Sharable
    public static class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

        @Override
        public void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out)
                throws Exception {
            try {
                // 将header序列化为JSON
                String commandJSON = JSON.toJSONString(remotingCommand);
                byte[] commandData = commandJSON.getBytes(StandardCharsets.UTF_8);

                // 计算长度
                int headerLength = commandData.length;
                int totalLength = 4 + headerLength; // header长度字段 + header + body

                // 写入总长度（包括header长度字段和body）
                out.writeInt(totalLength);

                // 写入header长度（包含类型信息，这里使用JSON类型=0）
                out.writeInt(headerLength | (0 << 24)); // 高8位为类型，低24位为长度

                // 写入header数据
                out.writeBytes(commandData);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

}

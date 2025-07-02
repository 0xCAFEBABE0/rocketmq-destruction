package org.gnor.rocketmq.broker_1;

import com.alibaba.fastjson2.JSON;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.gnor.rocketmq.common_1.RemotingCommand;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 * @since 2025/7/1
 */
public class BrokerStartup {
    private final ServerBootstrap serverBootstrap;
    protected final EventLoopGroup eventLoopGroupSelector;
    protected final EventLoopGroup eventLoopGroupBoss;
    protected final NettyServerHandler serverHandler;

    private static List<RemotingCommand> storeMsgs = new ArrayList<>();

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
            System.out.println("Broker started, listening 0.0.0.0:9111");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BrokerStartup brokerStartup = new BrokerStartup();
        brokerStartup.start();
    }

    public static class ThreadFactoryImpl implements ThreadFactory {
        private final AtomicLong threadIndex = new AtomicLong(0);
        private final String threadNamePrefix;
        private final boolean daemon;

        public ThreadFactoryImpl(final String threadNamePrefix) {
            this(threadNamePrefix, false);
        }

        public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon) {
            this.threadNamePrefix = threadNamePrefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
            thread.setDaemon(daemon);
            return thread;
        }
    }

    protected void initServerBootstrap(ServerBootstrap serverBootstrap) {
        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress("0.0.0.0", 9111))
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
    public static class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) {
            System.out.println("Received hey:" + remotingCommand.getHey());

            if (RemotingCommand.REQUEST_FLAG == remotingCommand.getFlag()) {
                if (RemotingCommand.PRODUCER_MSG == remotingCommand.getCode()) {
                    storeMsgs.add(remotingCommand);

                    RemotingCommand response = new RemotingCommand();
                    response.setFlag(RemotingCommand.RESPONSE_FLAG);
                    response.setHey("Response echo!!!!");
                    channelHandlerContext.channel().writeAndFlush(response);

                } else if (RemotingCommand.CONSUMER_MSG == remotingCommand.getCode()) {
                    RemotingCommand response = new RemotingCommand();
                    response.setFlag(RemotingCommand.RESPONSE_FLAG);
                    response.setHey(storeMsgs.get(0).getHey());
                    channelHandlerContext.channel().writeAndFlush(response);
                    storeMsgs.remove(0);
                }
            }

        }
    }

}

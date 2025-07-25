package org.gnor.rocketmq.npbc_consumermodel_10.remoting;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.*;
import org.gnor.rocketmq.common_1.NettyDecoder;
import org.gnor.rocketmq.common_1.NettyEncoder;
import org.gnor.rocketmq.common_1.RemotingCommand;
import org.gnor.rocketmq.common_1.ThreadFactoryImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NettyRemotingClient {
    private final EventLoopGroup eventLoopGroupWorker;
    private Bootstrap bootstrap;

    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<>();
    private final List<ResponseFuture> responseFutures = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(4, new ThreadFactoryImpl("NettyClientPublicExecutor_"));
    ;
    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ClientHouseKeepingService"));


    public NettyRemotingClient() {
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyClientSelector_"));

        //补偿
        TimerTask timerTaskScanResponseTable = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    System.out.println("TimerTaskScanResponseTable run error");
                } finally {
                    timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskScanResponseTable, 1000 * 3, TimeUnit.MILLISECONDS);
    }


    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis) throws InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        Attribute<String> att = channel.attr(AttributeKey.valueOf("RemoteAddr"));
        String channelRemoteAddr = att.get();
        if (channel != null && channel.isActive()) {
            long left = timeoutMillis;

            long costTime = System.currentTimeMillis() - beginStartTime;
            left -= costTime;
            if (left <= 0) {
                System.out.println("invokeSync call the addr[" + channelRemoteAddr + "] timeout");
            }
            RemotingCommand response = this.invokeSyncImpl(channel, request, left);
            return response;
        } else {
            return null;
        }
    }

    private RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                           final long timeoutMillis) throws InterruptedException {
        try {
            return invoke0(channel, request, timeoutMillis).thenApply(ResponseFuture::getResponseCommand)
                    .get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<ResponseFuture> it = this.responseFutures.iterator();
        while (it.hasNext()) {
            ResponseFuture rep = it.next();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                System.out.println("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                System.out.println("scanResponseTable, operationComplete Exception");
            }
        }
    }

    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;

        if (executor != null && !executor.isShutdown()) {
            try {
                executor.submit(() -> {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        System.out.println("executeInvokeCallback Exception");
                    } finally {
                        responseFuture.release();
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                System.out.println("scanResponseTable, operationComplete Exception");
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                System.out.println("executeInvokeCallback Exception");
            } finally {
                responseFuture.release();
            }
        }
    }

    private CompletableFuture<ResponseFuture> invoke0(final Channel channel, final RemotingCommand request,
                                                      final long timeoutMillis) {
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        long beginStartTime = System.currentTimeMillis();

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeoutMillis < costTime) {
            System.out.println("invokeAsyncImpl call the addr[" + channel.remoteAddress() + "] timeout");
            future.completeExceptionally(new RuntimeException("invokeAsyncImpl call timeout"));
            return future;
        }

        AtomicReference<ResponseFuture> responseFutureReference = new AtomicReference<>();
        final ResponseFuture responseFuture = new ResponseFuture(channel, 0, request, timeoutMillis - costTime,
                new InvokeCallback() {
                    @Override
                    public void operationComplete(ResponseFuture responseFuture) {

                    }

                    @Override
                    public void operationSucceed(RemotingCommand response) {
                        future.complete(responseFutureReference.get());
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                });
        responseFutureReference.set(responseFuture);
        this.responseFutures.add(responseFuture);
        try {
            channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    responseFuture.setSendRequestOK(true);
                    return;
                }
                System.out.println("send a request command to channel <" + channel.remoteAddress() + "> failed.");
            });
            return future;
        } catch (Exception e) {
            System.out.println("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
            return future;
        }
    }


    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException {
        final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            System.out.println("invokeOneway: channel is null");
            return;
        }
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = channelFuture.channel();
                Attribute<String> att = channel.attr(AttributeKey.valueOf("RemoteAddr"));
                String channelRemoteAddr = att.get();
                if (channel != null && channel.isActive()) {
                    this.invokeOnewayImpl(channel, request, timeoutMillis);
                } else {
                    //this.closeChannel(addr, channel);
                }
            }
        });
    }

    private void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException {
        try {
            channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                    System.out.println("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                }
            });
        } catch (Exception e) {
            System.out.println("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
        }
    }

    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        ChannelFuture channelFuture = getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            return null;
        }
        return channelFuture.awaitUninterruptibly().channel();
    }

    private ChannelFuture getAndCreateChannelAsync(final String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannelFuture();
        }
        try {
            cw = this.channelTables.get(addr);
            if (cw != null) {
                if (cw.isOK() || !cw.getChannelFuture().isDone()) {
                    return cw.getChannelFuture();
                } else {
                    this.channelTables.remove(addr);
                }
            }
            return createChannel(addr).getChannelFuture();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        return null;
    }

    private ChannelWrapper createChannel(String addr) {
        ChannelFuture channelFuture = doConnect(addr);
        System.out.println("createChannel: begin to connect remote host[" + addr + "] asynchronously");
        ChannelWrapper cw = new ChannelWrapper(addr, channelFuture);
        this.channelTables.put(addr, cw);
        return cw;
    }

    public ChannelFuture doConnect(String addr) {
        String[] hostAndPort = getHostAndPort(addr);
        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);
        return fetchBootstrap().connect(host, port);
    }

    protected String[] getHostAndPort(String address) {
        int split = address.lastIndexOf(":");
        return split < 0 ? new String[]{address} : new String[]{address.substring(0, split), address.substring(split + 1)};
    }

    private Bootstrap fetchBootstrap() {
        if (null == this.bootstrap) {
            this.bootstrap = createBootstrap();
        }
        return this.bootstrap;
    }

    private Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast(
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 120),
                                new NettyClientHandler());
                    }
                });
        return bootstrap;
    }

    public class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            //processMessageReceived(ctx, msg);
            final ResponseFuture responseFuture = responseFutures.get(0);
            if (responseFuture != null) {
                responseFuture.setResponseCommand(msg);

                responseFutures.remove(responseFuture);

                if (responseFuture.getInvokeCallback() != null) {
                    executeInvokeCallback(responseFuture);
                } else {
                    responseFuture.putResponse(msg);
                    responseFuture.release();
                }
            } else {
                System.out.println("receive response, cmd=" + msg.getCode() + ", but not matched any request, address=" + ctx.channel().remoteAddress() + ", channelId=" + ctx.channel().id());
            }
        }
    }


    class ChannelWrapper {
        private final ReentrantReadWriteLock lock;
        private ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.
        private ChannelFuture channelToClose;
        private long lastResponseTime;
        private final String channelAddress;

        public ChannelWrapper(String address, ChannelFuture channelFuture) {
            this.lock = new ReentrantReadWriteLock();
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
            this.channelAddress = address;
        }

        public boolean isOK() {
            return getChannel() != null && getChannel().isActive();
        }

        public boolean isWritable() {
            return getChannel().isWritable();
        }

        public boolean isWrapperOf(Channel channel) {
            return this.channelFuture.channel() != null && this.channelFuture.channel() == channel;
        }

        private Channel getChannel() {
            return getChannelFuture().channel();
        }

        public ChannelFuture getChannelFuture() {
            lock.readLock().lock();
            try {
                return this.channelFuture;
            } finally {
                lock.readLock().unlock();
            }
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }

        public String getChannelAddress() {
            return channelAddress;
        }

    }
}

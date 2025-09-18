package org.gnor.rocketmq.cretry_rNPC_14.broker;

import com.alibaba.fastjson2.JSONObject;
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
import org.gnor.rocketmq.cretry_rNPC_14.broker.client.ConsumerManager;
import org.gnor.rocketmq.cretry_rNPC_14.broker.longpolling.RequestHoldService;
import org.gnor.rocketmq.cretry_rNPC_14.broker.processor.PullMessageProcessor;
import org.gnor.rocketmq.cretry_rNPC_14.broker.processor.SendBackProcessor;
import org.gnor.rocketmq.cretry_rNPC_14.broker.processor.SendMessageProcessor;
import org.gnor.rocketmq.cretry_rNPC_14.broker.schedule.ScheduleMessageService;
import org.gnor.rocketmq.cretry_rNPC_14.broker.store.ConsumerOffsetManager;
import org.gnor.rocketmq.cretry_rNPC_14.broker.store.MessageStore;
import org.gnor.rocketmq.cretry_rNPC_14.broker.store.ReputMessageService;
import org.gnor.rocketmq.cretry_rNPC_14.remoting.NettyRemotingClient;

import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

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

    /*v4版本新增：本地文件存储*/
    private MessageStore messageStore;

    /*v6版本新增：消费进度管理*/
    private ConsumerOffsetManager consumerOffsetManager;
    protected ScheduledExecutorService scheduledExecutorService;

    /*v8版本新增：namesrv客户端*/
    private NettyRemotingClient remotingClient = new NettyRemotingClient();

    /*v11版本新增：重平衡*/
    private ConsumerManager consumerManager = new ConsumerManager();
    private ScheduledExecutorService clientHousekeepingService= Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerManagerService"));

    /*release_1版本新增：消息拉取处理器*/
    private PullMessageProcessor pullMessageProcessor;
    private SendMessageProcessor sendMessageProcessor;
    private ReputMessageService reputMessageService;

    /*v12版本新增：消息延迟机制*/
    private ScheduleMessageService scheduleMessageService;

    /*v14 新增消费重试*/
    private SendBackProcessor sendBackProcessor;

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public RequestHoldService getRequestHoldService() {
        return this.requestHoldService;
    }

    public BrokerStartup() {
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupSelector = new NioEventLoopGroup(3, new ThreadFactoryImpl("NettyServerNIOSelector_"));
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyNIOBoss_"));
        this.serverHandler = new NettyServerHandler();

        this.requestHoldService = new RequestHoldService(this);
        this.consumerOffsetManager = new ConsumerOffsetManager();
        this.messageStore = new MessageStore(consumerOffsetManager);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumerOffsetPersistService"));

        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.reputMessageService = new ReputMessageService(this);
        this.scheduleMessageService = new ScheduleMessageService(this);

        this.sendBackProcessor = new SendBackProcessor(this);
    }

    public NettyRemotingClient getRemotingClient() {
        return remotingClient;
    }

    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102"; // Will be created at broker when isAutoCreateTopicEnable

    public void start() {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setFlag(RemotingCommand.REQUEST_FLAG);
        remotingCommand.setCode(RemotingCommand.REGISTER_BROKER);
        remotingCommand.setBrokerName("Broker-01");
        remotingCommand.setBrokerAddr("127.0.0.1:9011");
        remotingCommand.setTopic(AUTO_CREATE_TOPIC_KEY_TOPIC);
        remotingCommand.setTopicQueueNums(4);
        try {
            RemotingCommand responseCmd = this.remotingClient.invokeSync("127.0.0.1:9091", remotingCommand, 30000L);
            System.out.println(responseCmd.getHey());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        this.consumerOffsetManager.load();

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            this.consumerOffsetManager.persist();
        }, 1000 * 10, 1000 * 5, TimeUnit.MILLISECONDS);
        initServerBootstrap(serverBootstrap);


        clientHousekeepingService.scheduleAtFixedRate(() -> {
            this.consumerManager.scanNotActivateChannel();
        }, 1000, 10_000, TimeUnit.MILLISECONDS);

        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            System.out.println("Broker started, listening 0.0.0.0:" + addr.getPort());

            new Thread(requestHoldService).start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        scheduleMessageService.start();
        reputMessageService.start();
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
                response.setOpaque(remotingCommand.getOpaque());
                switch (remotingCommand.getCode()) {
                    case RemotingCommand.PRODUCER_MSG:
                        sendMessageProcessor.processRequest(channelHandlerContext.channel(), remotingCommand, response);
                        break;
                    case RemotingCommand.CONSUMER_MSG:
                        pullMessageProcessor.processRequest(channelHandlerContext.channel(), remotingCommand);
                        break;
                    case RemotingCommand.QUERY_CONSUMER_OFFSET:
                        long offset = consumerOffsetManager.queryOffset(remotingCommand.getTopic(), remotingCommand.getQueueId());
                        response.setConsumerOffset(offset);
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setCode(RemotingCommand.QUERY_CONSUMER_OFFSET);
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    case RemotingCommand.HEART_BEAT:
                        Map<String, String> propMap = JSONObject.parseObject(remotingCommand.getProperties(), Map.class);
                        String tag = null != propMap && propMap.containsKey("TAG") ? propMap.get("TAG") : "";
                        System.out.println("HEART_BEAT: " + remotingCommand.getClientId() + " " + remotingCommand.getTopic() + " " + tag + ": " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        consumerManager.registerConsumer(remotingCommand.getClientId(), channelHandlerContext.channel(), remotingCommand.getTopic(), Collections.singleton(tag));
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setHey("Broker registered!");
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    case RemotingCommand.GET_CONSUMER_LIST_BY_GROUP:
                        List<String> consumerList = consumerManager.getConsumerListByTopic(remotingCommand.getTopic());
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setHey(JSONObject.toJSONString(consumerList));
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    case RemotingCommand.UPDATE_CONSUMER_OFFSET:
                        consumerOffsetManager.commitOffset(remotingCommand.getTopic(), remotingCommand.getCommitOffset(), remotingCommand.getQueueId());
                        response.setFlag(RemotingCommand.RESPONSE_FLAG);
                        response.setHey("Update consumer offset!");
                        channelHandlerContext.channel().writeAndFlush(response);
                        break;
                    case RemotingCommand.CONSUMER_SEND_MSG_BACK:
                        sendBackProcessor.processRequest(channelHandlerContext.channel(), remotingCommand);
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

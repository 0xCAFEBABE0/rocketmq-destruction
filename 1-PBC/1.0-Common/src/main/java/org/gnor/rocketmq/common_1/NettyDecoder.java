package org.gnor.rocketmq.common_1;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * @version 1.0
 * @since 2025/7/3
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
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

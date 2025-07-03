package org.gnor.rocketmq.common_1;

import com.alibaba.fastjson2.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * @version 1.0
 * @since 2025/7/3
 */
@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    @Override
    public void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) {
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

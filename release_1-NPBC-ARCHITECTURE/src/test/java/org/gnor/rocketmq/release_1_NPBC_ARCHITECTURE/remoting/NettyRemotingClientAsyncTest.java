package org.gnor.rocketmq.release_1_NPBC_ARCHITECTURE.remoting;

import org.gnor.rocketmq.common_1.RemotingCommand;
import org.junit.Test;

/**
 * 测试新增的异步接口
 */
public class NettyRemotingClientAsyncTest {

    @Test
    public void testInvokeAsync() {
        NettyRemotingClient client = new NettyRemotingClient();
        RemotingCommand request = new RemotingCommand();
        request.setCode(1);
        
        // 测试新增的异步接口
        try {
            client.invokeAsync("127.0.0.1:9876", request, 3000, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    System.out.println("异步调用完成");
                }
                
                @Override
                public void operationSucceed(RemotingCommand response) {
                    System.out.println("异步调用成功: " + response);
                }
                
                @Override
                public void operationFail(Throwable throwable) {
                    System.out.println("异步调用失败: " + throwable.getMessage());
                }
            });
            
            // 等待一段时间让异步操作完成
            Thread.sleep(1000);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

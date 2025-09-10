package org.gnor.rocketmq.pretry_rNPC_13.producer;

public class SendResult {
    private SendStatus sendStatus;
    private String msgId;

    public SendResult(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public SendResult(SendStatus sendStatus, String msgId) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
    }
    public SendStatus getSendStatus() {
        return sendStatus;
    }
    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }
    public String getMsgId() {
        return msgId;
    }
    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
}

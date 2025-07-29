/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gnor.rocketmq.npbc_lb_11.broker.client;

import io.netty.channel.Channel;

public class ClientChannelInfo {
    private final Channel channel;
    private final String clientId;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ClientChannelInfo(Channel channel) {
        this(channel, null);
    }

    public ClientChannelInfo(Channel channel, String clientId) {
        this.channel = channel;
        this.clientId = clientId;
    }


    public Channel getChannel() {
        return channel;
    }

    public String getClientId() {
        return clientId;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        result = prime * result + (int) (lastUpdateTimestamp ^ (lastUpdateTimestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClientChannelInfo other = (ClientChannelInfo) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        } else if (this.channel != other.channel) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ClientChannelInfo [channel=" + channel + ", clientId=" + clientId + ", lastUpdateTimestamp=" + lastUpdateTimestamp + "]";
    }
}

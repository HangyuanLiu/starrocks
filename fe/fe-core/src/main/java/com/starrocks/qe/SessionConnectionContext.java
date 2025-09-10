// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe;

import com.starrocks.mysql.MysqlChannel;

import java.util.UUID;

/**
 * SessionConnectionContext encapsulates connection channel and identifiers for a session
 * (mysqlChannel, remoteIP, sessionId, connectionId).
 */
public class SessionConnectionContext {
    private MysqlChannel mysqlChannel;
    private String remoteIP;
    private UUID sessionId;
    private int connectionId;
    private String proxyHostName;
    private volatile boolean closed;

    public MysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public void setMysqlChannel(MysqlChannel mysqlChannel) {
        this.mysqlChannel = mysqlChannel;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public String getProxyHostName() {
        return proxyHostName;
    }

    public void setProxyHostName(String proxyHostName) {
        this.proxyHostName = proxyHostName;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }
}



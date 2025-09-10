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

import com.starrocks.mysql.MysqlCommand;
import com.starrocks.thrift.TUniqueId;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SessionExecutionContext {
    // The identifier of the statement being analyzed/executed. Must be set before analyze.
    private long stmtId;
    private long forwardedStmtId;
    private int forwardTimes;
    // The queryId of the last query processed by this session. In some scenarios,
    // the user can get the output of a request by queryId, such as INSERT or EXPORT requests.
    private UUID lastQueryId;
    // The queryId is used to track a user's request across the entire system. A user request will only
    // have one queryId in the entire StarRocks system. In some scenarios, a user request may be forwarded
    // to multiple nodes for processing or be processed repeatedly, but each execution instance will share
    // the same queryId.
    private UUID queryId;
    // A request can be executed multiple times because of retry or redirect. This id is used to
    // distinguish between different execution instances.
    private TUniqueId executionId;

    // Connection identifiers/state
    // id for this connection
    private int connectionId;
    // Time when the connection is made
    private long connectionStartTime;
    // Command this connection is processing.
    private MysqlCommand command = MysqlCommand.COM_SLEEP;

    // Timing
    // last command start time
    private Instant startTime = Instant.now();
    // last command end time
    private Instant endTime = Instant.ofEpochMilli(0);
    // last command pending time(s), query's timeout should not contain pending time.
    private int pendingTimeSecond = 0;

    // Counters
    private long returnRows = 0;
    private final AtomicInteger pendingForwardRequests = new AtomicInteger(0);

    // Flags
    private volatile boolean isPending = false;
    private volatile boolean isForward = false;

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public long getForwardedStmtId() {
        return forwardedStmtId;
    }

    public void setForwardedStmtId(long forwardedStmtId) {
        this.forwardedStmtId = forwardedStmtId;
    }

    public int getForwardTimes() {
        return forwardTimes;
    }

    public void setForwardTimes(int forwardTimes) {
        this.forwardTimes = forwardTimes;
    }

    public UUID getLastQueryId() {
        return lastQueryId;
    }

    public void setLastQueryId(UUID lastQueryId) {
        this.lastQueryId = lastQueryId;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public void setQueryId(UUID queryId) {
        this.queryId = queryId;
    }

    public TUniqueId getExecutionId() {
        return executionId;
    }

    public void setExecutionId(TUniqueId executionId) {
        this.executionId = executionId;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public long getConnectionStartTime() {
        return connectionStartTime;
    }

    public void resetConnectionStartTime() {
        this.connectionStartTime = System.currentTimeMillis();
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime.toEpochMilli();
    }

    public Instant getStartTimeInstant() {
        return startTime;
    }

    public void setStartTime() {
        startTime = Instant.now();
        pendingTimeSecond = 0;
    }

    public void setStartTime(Instant start) {
        startTime = start;
        pendingTimeSecond = 0;
    }

    public void setEndTime() {
        endTime = Instant.now();
    }

    public Instant getEndTimeInstant() {
        return endTime;
    }

    public int getPendingTimeSecond() {
        return pendingTimeSecond;
    }

    public void setPendingTimeSecond(int pendingTimeSecond) {
        this.pendingTimeSecond = pendingTimeSecond;
    }

    public boolean isPending() {
        return isPending;
    }

    public void setPending(boolean pending) {
        isPending = pending;
    }

    public boolean isForward() {
        return isForward;
    }

    public void setForward(boolean forward) {
        isForward = forward;
    }

    // Backward-compatible alias matching existing usage
    public void setIsForward(boolean forward) {
        setForward(forward);
    }

    // Return rows helpers
    public void updateReturnRows(int delta) {
        this.returnRows += delta;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetReturnRows() {
        this.returnRows = 0;
    }

    // Forward request counters
    public boolean hasPendingForwardRequest() {
        return pendingForwardRequests.intValue() > 0;
    }

    public void incPendingForwardRequest() {
        pendingForwardRequests.incrementAndGet();
    }

    public void decPendingForwardRequest() {
        pendingForwardRequests.decrementAndGet();
    }
}



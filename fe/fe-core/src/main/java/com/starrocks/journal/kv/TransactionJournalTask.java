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
package com.starrocks.journal.kv;

import com.sleepycat.je.Transaction;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalTask;

import java.util.List;

public class TransactionJournalTask extends JournalTask {
    private Transaction transaction = null;
    private List<Pair<Short, Writable>> opLogs;

    public TransactionJournalTask(Transaction transaction, List<Pair<Short, Writable>> opLogs) {
        super(null, -1);
        this.transaction = transaction;
        this.opLogs = opLogs;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public List<Pair<Short, Writable>> getOperationType() {
        return opLogs;
    }
}

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

package com.starrocks.transaction;

import com.starrocks.sql.ast.DmlStmt;

import java.util.ArrayList;
import java.util.List;

public class GlobalTransactionState {
    private TransactionState transactionState;
    private final List<TransactionStateItem> transactionStateItems = new ArrayList<>();

    public GlobalTransactionState() {
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }

    public void setTransactionState(TransactionState transactionState) {
        this.transactionState = transactionState;
    }

    public List<TransactionStateItem> getTransactionStateItems() {
        return transactionStateItems;
    }

    public void addTransactionItem(TransactionStateItem transactionStateItem) {
        transactionStateItems.add(transactionStateItem);
    }

    public static class TransactionStateItem {
        private long loadedRows;
        private long loadedBytes;
        private long filteredRows;

        private List<TabletCommitInfo> tabletCommitInfos = null;
        private List<TabletFailInfo> tabletFailInfos = null;

        private DmlStmt dmlStmt;

        public void addLoadedRows(long loadedRows) {
            this.loadedRows += loadedRows;
        }

        public long getLoadedRows() {
            return loadedRows;
        }

        public void addLoadedBytes(long loadedBytes) {
            this.loadedBytes += loadedBytes;
        }

        public long getLoadedBytes() {
            return loadedBytes;
        }

        public void addFilteredRows(long filteredRows) {
            this.filteredRows += filteredRows;
        }

        public long getFilteredRows() {
            return filteredRows;
        }

        public List<TabletCommitInfo> getTabletCommitInfos() {
            return tabletCommitInfos;
        }

        public void setTabletCommitInfos(List<TabletCommitInfo> tabletCommitInfos) {
            this.tabletCommitInfos = tabletCommitInfos;
        }

        public List<TabletFailInfo> getTabletFailInfos() {
            return tabletFailInfos;
        }

        public void setTabletFailInfos(List<TabletFailInfo> tabletFailInfos) {
            this.tabletFailInfos = tabletFailInfos;
        }

        public DmlStmt getDmlStmt() {
            return dmlStmt;
        }

        public void setDmlStmt(DmlStmt dmlStmt) {
            this.dmlStmt = dmlStmt;
        }
    }
}

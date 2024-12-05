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

import com.google.common.primitives.Ints;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.BeginStmt;
import com.starrocks.sql.ast.CommitStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.RollbackStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.GlobalTransactionState;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransactionStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(TransactionStmtExecutor.class);

    public static void beginStmt(ConnectContext context, BeginStmt stmt) throws BeginTransactionException {
        long transactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionIDGenerator().getNextTransactionId();
        String label = MetaUtils.genInsertLabel(context.getExecutionId());
        TransactionState transactionState = new TransactionState(
                transactionId, label, null,
                TransactionState.LoadJobSourceType.INSERT_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE,
                        FrontendOptions.getLocalHostAddress()),
                context.getExecTimeout());
        transactionState.setPrepareTime(System.currentTimeMillis());
        transactionState.setWarehouseId(context.getCurrentWarehouseId());
        boolean combinedTxnLog =
                LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING);
        transactionState.setUseCombinedTxnLog(combinedTxnLog);

        GlobalTransactionState globalTransactionState = new GlobalTransactionState();
        globalTransactionState.setTransactionState(transactionState);
        context.setGlobalTransactionLoadState(globalTransactionState);

        context.getState().setOk(0, 0, buildMessage(label, TransactionStatus.PREPARE, transactionId, -1));
    }

    public static void loadData(Database database,
                                Table targetTable,
                                ExecPlan execPlan,
                                DmlStmt dmlStmt,
                                OriginStatement originStmt,
                                ConnectContext context) {
        try {
            Coordinator coord = new DefaultCoordinator.Factory().createInsertScheduler(
                    context, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift());

            GlobalTransactionState globalTransactionState = context.getGlobalTransactionLoadState();
            TransactionState transactionState = globalTransactionState.getTransactionState();
            if (transactionState.getDbId() == 0) {
                transactionState.setDbId(database.getId());
                DatabaseTransactionMgr databaseTransactionMgr =
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                .getDatabaseTransactionMgr(database.getId());
                databaseTransactionMgr.upsertTransactionState(transactionState);
            }

            transactionState.addTableIdList(targetTable.getId());

            GlobalTransactionState.TransactionStateItem item =
                    DMLStmtExecutor.load(database, targetTable, execPlan, dmlStmt, originStmt, context, coord);
            globalTransactionState.addTransactionItem(item);

            context.getState().setOk(item.getLoadedRows(), Ints.saturatedCast(item.getFilteredRows()),
                    buildMessage(transactionState.getLabel(), TransactionStatus.PREPARE,
                            transactionState.getTransactionId(), database.getId()));
        } catch (StarRocksException | RpcException | InterruptedException e) {
            rollbackStmt(context, null);
        }
    }

    public static void commitStmt(ConnectContext context, CommitStmt stmt) {
        long transactionId = context.getGlobalTransactionLoadState().getTransactionState().getTransactionId();
        TransactionState transactionState = context.getGlobalTransactionLoadState().getTransactionState();
        long databaseId = transactionState.getDbId();
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseId);
        try {
            DMLStmtExecutor.commit(transactionId, database, context);
        } catch (StarRocksException | LockTimeoutException e) {
            LOG.warn("errors when abort txn", e);
        }
    }

    public static void rollbackStmt(ConnectContext context, RollbackStmt stmt) {
        long transactionId = context.getGlobalTransactionLoadState().getTransactionState().getTransactionId();
        try {
            DMLStmtExecutor.rollback(context, transactionId, "rollback transaction by user");
        } catch (StarRocksException e) {
            // just print a log if abort txn failed. This failure do not need to pass to user.
            // user only concern abort how txn failed.
            LOG.warn("errors when abort txn", e);
        }
    }

    public static String buildMessage(String label, TransactionStatus txnStatus, long transactionId, long databaseId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("'label':'").append(label).append("', ");
        sb.append("'status':'").append(txnStatus.name()).append("', ");
        sb.append("'txnId':'").append(transactionId).append("'");

        if (txnStatus == TransactionStatus.COMMITTED) {
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            String timeoutInfo = transactionMgr.getTxnPublishTimeoutDebugInfo(databaseId, transactionId);
            LOG.warn("txn {} publish timeout {}", transactionId, timeoutInfo);
            if (timeoutInfo.length() > 240) {
                timeoutInfo = timeoutInfo.substring(0, 240) + "...";
            }
            String errMsg = "Publish timeout " + timeoutInfo;

            sb.append(", 'err':'").append(errMsg).append("'");
        }

        sb.append("}");

        return sb.toString();
    }
}

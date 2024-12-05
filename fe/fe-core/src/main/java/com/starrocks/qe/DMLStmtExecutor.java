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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.LoadException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.load.InsertOverwriteJobMgr;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.persist.CreateInsertOverwriteJobLog;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.transaction.ExplicitTxnCommitAttachment;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.GlobalTransactionState;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.transaction.VisibleStateWaiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.starrocks.common.ErrorCode.ERR_TXN_NOT_EXIST;

public class DMLStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(DMLStmtExecutor.class);

    public static GlobalTransactionState.TransactionStateItem load(
            Database database,
            Table targetTable,
            ExecPlan execPlan,
            DmlStmt dmlStmt,
            OriginStatement originStmt,
            ConnectContext context,
            Coordinator coord) throws StarRocksException, InterruptedException, RpcException {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        MetricRepo.COUNTER_LOAD_ADD.increase(1L);

        // Every time set no send flag and clean all data in buffer
        if (context.getMysqlChannel() != null) {
            context.getMysqlChannel().reset();
        }

        long transactionId = dmlStmt.getTxnId();
        TransactionState txnState = transactionMgr.getTransactionState(database.getId(), transactionId);
        if (txnState == null) {
            throw ErrorReportException.report(ERR_TXN_NOT_EXIST, transactionId);
        }
        if (!txnState.getTableIdList().contains(targetTable.getId())) {
            txnState.getTableIdList().add(targetTable.getId());
            txnState.addTableIndexes((OlapTable) targetTable);
        }

        String label = txnState.getLabel();
        long jobId = -1;
        if (execPlan.getScanNodes().stream()
                .anyMatch(scanNode -> scanNode instanceof OlapScanNode || scanNode instanceof FileScanNode)) {
            coord.setLoadJobType(TLoadJobType.INSERT_QUERY);
        } else {
            coord.setLoadJobType(TLoadJobType.INSERT_VALUES);
        }

        InsertLoadJob loadJob = context.getGlobalStateMgr().getLoadMgr().registerInsertLoadJob(
                label,
                database.getFullName(),
                targetTable.getId(),
                transactionId,
                DebugUtil.printId(context.getExecutionId()),
                context.getQualifiedUser(),
                EtlJobType.INSERT,
                System.currentTimeMillis(),
                estimate(execPlan),
                context.getSessionVariable().getQueryTimeoutS(),
                coord);
        loadJob.setJobProperties(dmlStmt.getProperties());
        jobId = loadJob.getId();
        txnState.setCallbackId(jobId);
        coord.setLoadJobId(jobId);

        QeProcessorImpl.QueryInfo queryInfo =
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord);
        QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), queryInfo);

        try {
            coord.exec();
        } catch (Exception e) {

        }
        //coord.setTopProfileSupplier(this::buildTopLevelProfile);
        coord.setExecPlan(execPlan);

        int timeout = context.getSessionVariable().getQueryTimeoutS();
        coord.join(timeout);
        if (!coord.isDone()) {
            /*
             * In this case, There are two factors that lead query cancelled:
             * 1: TIMEOUT
             * 2: BE EXCEPTION
             * So we should distinguish these two factors.
             */
            if (!coord.checkBackendState()) {
                // When enable_collect_query_detail_info is set to true, the plan will be recorded in the query detail,
                // and hence there is no need to log it here.
                if (Config.log_plan_cancelled_by_crash_be && context.getQueryDetail() == null) {
                    LOG.warn("Query cancelled by crash of backends [QueryId={}] [SQL={}] [Plan={}]",
                            DebugUtil.printId(context.getExecutionId()),
                            originStmt == null ? "" : originStmt.originStmt,
                            execPlan.getExplainString(TExplainLevel.COSTS));
                }

                coord.cancel(ErrorCode.ERR_QUERY_CANCELLED_BY_CRASH.formatErrorMsg());
                throw new NoAliveBackendException();
            } else {
                coord.cancel(ErrorCode.ERR_TIMEOUT.formatErrorMsg(getExecType(dmlStmt), timeout, ""));
                if (coord.isThriftServerHighLoad()) {
                    throw new TimeoutException(getExecType(dmlStmt),
                            timeout,
                            "Please check the thrift-server-pool metrics, " +
                                    "if the pool size reaches thrift_server_max_worker_threads(default is 4096), " +
                                    "you can set the config to a higher value in fe.conf, " +
                                    "or set parallel_fragment_exec_instance_num to a lower value in session variable");
                } else {
                    throw new TimeoutException(getExecType(dmlStmt), timeout,
                            String.format("please increase the '%s' session variable and retry",
                                    SessionVariable.INSERT_TIMEOUT));
                }
            }
        }

        if (!coord.getExecStatus().ok()) {
            throw new LoadException(ErrorCode.ERR_FAILED_WHEN_INSERT,
                    coord.getExecStatus().getErrorMsg().isEmpty() ?
                            coord.getExecStatus().getErrorCodeString() : coord.getExecStatus().getErrorMsg());
        }

        LOG.debug("delta files is {}", coord.getDeltaUrls());

        loadJob.updateLoadingStatus(coord.getLoadCounters());

        long loadedRows = coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null ?
                Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL)) : 0;

        // filteredRows is stored in int64_t in the backend, so use long here.
        long filteredRows = coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null ?
                Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL)) : 0;

        // insert will fail if 'filtered rows / total rows' exceeds max_filter_ratio
        // for native table and external catalog table(without insert load job)
        if (filteredRows > (filteredRows + loadedRows) * dmlStmt.getMaxFilterRatio()) {
            throw new LoadException(ErrorCode.ERR_LOAD_HAS_FILTERED_DATA, "");
        }

        long loadedBytes = coord.getLoadCounters().get(LoadJob.LOADED_BYTES) != null ?
                Long.parseLong(coord.getLoadCounters().get(LoadJob.LOADED_BYTES)) : 0;

        GlobalTransactionState.TransactionStateItem item =
                new GlobalTransactionState.TransactionStateItem();
        item.setDmlStmt(dmlStmt);
        item.setTabletCommitInfos(TabletCommitInfo.fromThrift(coord.getCommitInfos()));
        item.setTabletFailInfos(TabletFailInfo.fromThrift(coord.getFailInfos()));
        item.addLoadedRows(loadedRows);
        item.addFilteredRows(filteredRows);
        item.addLoadedBytes(loadedBytes);
        return item;
    }

    public static void commit(
            long transactionId,
            Database database,
            ConnectContext context) throws StarRocksException, LockTimeoutException {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        TransactionState txnState = transactionMgr.getTransactionState(database.getId(), transactionId);
        GlobalTransactionState globalTransactionState = context.getGlobalTransactionLoadState();

        int timeout = context.getSessionVariable().getQueryTimeoutS();
        long jobDeadLineMs = System.currentTimeMillis() + timeout * 1000L;

        List<TabletCommitInfo> commitInfos = Lists.newArrayList();
        List<TabletFailInfo> failInfos = Lists.newArrayList();
        long loadedRows = 0;
        for (GlobalTransactionState.TransactionStateItem item : globalTransactionState.getTransactionStateItems()) {
            commitInfos.addAll(item.getTabletCommitInfos());
            failInfos.addAll(item.getTabletFailInfos());
            loadedRows += item.getLoadedRows();
        }

        TxnCommitAttachment txnCommitAttachment = new ExplicitTxnCommitAttachment(loadedRows);
        VisibleStateWaiter visibleWaiter = transactionMgr.retryCommitOnRateLimitExceeded(
                database,
                transactionId,
                commitInfos,
                failInfos,
                txnCommitAttachment,
                timeout);

        long publishWaitMs = Config.enable_sync_publish ? jobDeadLineMs - System.currentTimeMillis() :
                context.getSessionVariable().getTransactionVisibleWaitTimeout() * 1000;

        TransactionStatus txnStatus;
        if (visibleWaiter.await(publishWaitMs, TimeUnit.MILLISECONDS)) {
            txnStatus = TransactionStatus.VISIBLE;
        } else {
            txnStatus = TransactionStatus.COMMITTED;
        }

        List<GlobalTransactionState.TransactionStateItem> transactionStateItems
                = globalTransactionState.getTransactionStateItems();
        List<Long> callbackIds = txnState.getCallbackId();
        Preconditions.checkArgument(transactionStateItems.size() == callbackIds.size());

        for (int i = 0; i < transactionStateItems.size(); i++) {
            GlobalTransactionState.TransactionStateItem item = transactionStateItems.get(i);

            DmlStmt dmlStmt = item.getDmlStmt();
            Table targetTable = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(database.getFullName(), dmlStmt.getTableName().getTbl());

            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
            // collect table-level metrics
            TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(targetTable.getId());
            entity.counterInsertLoadFinishedTotal.increase(1L);
            entity.counterInsertLoadRowsTotal.increase(item.getLoadedRows());
            entity.counterInsertLoadBytesTotal.increase(item.getLoadedBytes());

            GlobalStateMgr.getCurrentState().getOperationListenerBus()
                    .onDMLStmtJobTransactionFinish(txnState, database, targetTable, DmlType.fromStmt(dmlStmt));

            context.getGlobalStateMgr().getLoadMgr().recordFinishedOrCancelledLoadJob(
                    callbackIds.get(i),
                    EtlJobType.INSERT,
                    "",
                    "");
            //coord.getTrackingUrl());
        }

        context.getState().setOk(0, 0,
                buildMessage(txnState.getLabel(), txnStatus, transactionId, database.getId()));
    }

    public static void rollback(ConnectContext context, long transactionId, String errMsg) throws StarRocksException {
        GlobalTransactionState globalTransactionState = context.getGlobalTransactionLoadState();
        TransactionState transactionState = context.getGlobalTransactionLoadState().getTransactionState();
        long databaseId = transactionState.getDbId();

        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        List<TabletCommitInfo> commitInfos = Lists.newArrayList();
        List<TabletFailInfo> failInfos = Lists.newArrayList();
        for (GlobalTransactionState.TransactionStateItem item : globalTransactionState.getTransactionStateItems()) {
            commitInfos.addAll(item.getTabletCommitInfos());
            failInfos.addAll(item.getTabletFailInfos());
        }

        transactionMgr.abortTransaction(
                databaseId,
                transactionId,
                errMsg,
                commitInfos,
                failInfos,
                null);
    }

    public static LoadMgr.EstimateStats estimate(ExecPlan execPlan) {
        long estimateScanRows = -1;
        int estimateFileNum = 0;
        long estimateScanFileSize = 0;

        boolean needQuery = false;
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            if (scanNode instanceof OlapScanNode) {
                estimateScanRows += ((OlapScanNode) scanNode).getActualRows();
                needQuery = true;
            }
            if (scanNode instanceof FileScanNode) {
                estimateFileNum += ((FileScanNode) scanNode).getFileNum();
                estimateScanFileSize += ((FileScanNode) scanNode).getFileTotalSize();
                needQuery = true;
            }
        }

        if (needQuery) {
            estimateScanRows = execPlan.getFragments().get(0).getPlanRoot().getCardinality();
        }

        return new LoadMgr.EstimateStats(estimateScanRows, estimateFileNum, estimateScanFileSize);
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

    public static String getExecType(StatementBase stmt) {
        if (stmt instanceof InsertStmt || stmt instanceof CreateTableAsSelectStmt) {
            return "Insert";
        } else if (stmt instanceof UpdateStmt) {
            return "Update";
        } else if (stmt instanceof DeleteStmt) {
            return "Delete";
        } else {
            return "Query";
        }
    }

    public static void handleInsertOverwrite(ConnectContext context, StmtExecutor stmtExecutor, InsertStmt
            insertStmt)
            throws Exception {
        TableName tableName = insertStmt.getTableName();
        Database db =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableName.getCatalog(), tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }

        Locker locker = new Locker();
        Table table = insertStmt.getTargetTable();
        if (!(table instanceof OlapTable)) {
            LOG.warn("insert overwrite table:{} type:{} is not supported", table.getName(), table.getClass());
            throw new RuntimeException("not supported table type for insert overwrite");
        }
        OlapTable olapTable = (OlapTable) insertStmt.getTargetTable();
        InsertOverwriteJob job = new InsertOverwriteJob(GlobalStateMgr.getCurrentState().getNextId(),
                insertStmt, db.getId(), olapTable.getId(), context.getCurrentWarehouseId(),
                insertStmt.isDynamicOverwrite());
        if (!locker.lockDatabaseAndCheckExist(db, LockType.WRITE)) {
            throw new DmlException("database:%s does not exist.", db.getFullName());
        }
        try {
            // add an edit log
            CreateInsertOverwriteJobLog info = new CreateInsertOverwriteJobLog(job.getJobId(),
                    job.getTargetDbId(), job.getTargetTableId(), job.getSourcePartitionIds(),
                    job.isDynamicOverwrite());
            GlobalStateMgr.getCurrentState().getEditLog().logCreateInsertOverwrite(info);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
        insertStmt.setOverwriteJobId(job.getJobId());
        InsertOverwriteJobMgr manager = GlobalStateMgr.getCurrentState().getInsertOverwriteJobMgr();
        manager.executeJob(context, stmtExecutor, job);
    }

}

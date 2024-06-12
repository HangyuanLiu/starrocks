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
package com.starrocks.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.staros.proto.FilePathInfo;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.concurrent.CountingLatch;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LocalPartitionHandler {
    private static final Logger LOG = LogManager.getLogger(LocalPartitionHandler.class);

    public static void addPartitions(ConnectContext ctx, Database db, OlapTable table, AddPartitionClause addPartitionClause) throws DdlException {
        List<PartitionDesc> partitionDescList = addPartitionClause.getResolvedPartitionDescList();
        List<Partition> partitionList = Lists.newArrayList();

        Map<Long, MaterializedIndexMeta> materializedIndexMetaMapCopy = Maps.newHashMap();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.READ);
        try {

            for (Map.Entry<Long, MaterializedIndexMeta> entry : table.getIndexIdToMeta().entrySet()) {
                MaterializedIndexMeta materializedIndexMeta = entry.getValue();
                MaterializedIndexMeta mCopy = DeepCopy.copyWithGson(materializedIndexMeta, MaterializedIndexMeta.class);
                materializedIndexMetaMapCopy.put(entry.getKey(), mCopy);
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db, Lists.newArrayList(table.getId()), LockType.READ);
        }

        for (PartitionDesc partitionDesc : partitionDescList) {
            partitionList.add(createPartition(ctx, db, table, partitionDesc, null, materializedIndexMetaMapCopy));
        }

        for (Map.Entry<Long, MaterializedIndexMeta> entry : materializedIndexMetaMapCopy.entrySet()) {
            MaterializedIndexMeta materializedIndexMeta = entry.getValue();

            buildPartitions(
                    db.getId(),
                    table.getId(),
                    materializedIndexMeta,
                    partitionList,
                    ctx.getCurrentWarehouseId(),
                    partitionDescList.get(xxx),
                    table.isCloudNativeTableOrMaterializedView(),
                    table.getIndexes(),
                    table.getBfColumns(),
                    table.getBfFpp(),
                    table.enablePersistentIndex(),
                    table.getPersistentIndexType(),
                    table.primaryIndexCacheExpireSec(),
                    table.getCurBinlogConfig(),
                    table.getCompressionType());
        }
    }

    public static Partition createPartition(ConnectContext ctx,
                                            Database database, OlapTable table,
                                            PartitionDesc partitionDesc,
                                            DistributionInfo distributionInfo,
                                            Map<Long, MaterializedIndexMeta> indexIdToMeta) throws DdlException {
        long partitionId = GlobalStateMgr.getCurrentState().getNextId();

        if (table.isCloudNativeTableOrMaterializedView()) {
            return createLakeTablets(
                    database.getId(),
                    table.getId(),
                    partitionId,
                    partitionDesc,
                    distributionInfo,
                    indexIdToMeta,
                    null,
                    ctx.getCurrentWarehouseId(),
                    table.getPartitionFilePathInfo(partitionId));
        } else {
            return createOlapTablets(
                    database.getId(),
                    table.getId(),
                    partitionId,
                    partitionDesc,
                    distributionInfo,
                    indexIdToMeta,
                    partitionDesc.getVersionInfo(),
                    partitionDesc.getReplicationNum(),
                    null,
                    table.getLocation()
            );
        }
    }


    private static Partition createLakeTablets(long databaseId, long tableId, long partitionId,
                                               PartitionDesc partitionDesc,
                                               DistributionInfo distributionInfo,
                                               Map<Long, MaterializedIndexMeta> indexIdToMeta,
                                               Set<Long> tabletIdSet,
                                               long warehouseId,
                                               FilePathInfo pathInfo) throws DdlException {

        String partitionName = partitionDesc.getPartitionName();
        long shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(databaseId, tableId, partitionId);
        Partition partition = new Partition(
                partitionId,
                partitionName,
                null,
                distributionInfo,
                shardGroupId);
        Long partitionVersion = partitionDesc.getVersionInfo();
        if (partitionVersion != null) {
            partition.updateVisibleVersion(partitionVersion);
        }

        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            Long indexId = entry.getKey();
            MaterializedIndexMeta materializedIndexMeta = entry.getValue();

            MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);

            TabletMeta tabletMeta = new TabletMeta(
                    databaseId, tableId, partitionId, indexId,
                    materializedIndexMeta.getSchemaHash(),
                    partitionDesc.getPartitionDataProperty().getStorageMedium(),
                    true);

            DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
            if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH
                    && distributionInfoType != DistributionInfo.DistributionInfoType.RANDOM) {
                throw new DdlException("Unknown distribution type: " + distributionInfoType);
            }

            Map<String, String> properties = new HashMap<>();
            properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(tableId));
            properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(partitionId));
            properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
            int bucketNum = distributionInfo.getBucketNum();
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Optional<Long> workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(warehouseId);
            if (workerGroupId.isEmpty()) {
                Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
                ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, warehouse.getName());
            }

            List<Long> shardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                    bucketNum,
                    pathInfo,
                    partitionDesc.getDataCacheInfo().getCacheInfo(),
                    shardGroupId, null, properties, workerGroupId.get());
            for (long shardId : shardIds) {
                Tablet tablet = new LakeTablet(shardId);
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());
            }
        }

        return partition;
    }

    private static Partition createOlapTablets(
            long databaseId,
            long tableId,
            long partitionId,
            PartitionDesc partitionDesc,
            DistributionInfo distributionInfo,
            Map<Long, MaterializedIndexMeta> indexIdToMeta,
            long version,
            short replicationNum,
            Set<Long> tabletIdSet,
            Multimap<String, String> locReq) throws DdlException {
        Preconditions.checkArgument(replicationNum > 0);

        DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH
                && distributionInfoType != DistributionInfo.DistributionInfoType.RANDOM) {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }


        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        CountingLatch colocateTableCreateSyncer = GlobalStateMgr.getCurrentState().getLocalMetastore().getColocateTableCreateSyncer();

        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            Long indexId = entry.getKey();
            MaterializedIndexMeta materializedIndexMeta = entry.getValue();

            MaterializedIndex materializedIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);

            TabletMeta tabletMeta = new TabletMeta(
                    databaseId, tableId, partitionId, indexId,
                    materializedIndexMeta.getSchemaHash(),
                    partitionDesc.getPartitionDataProperty().getStorageMedium(),
                    false);

            List<List<Long>> backendsPerBucketSeq = null;
            ColocateTableIndex.GroupId groupId = null;
            boolean initBucketSeqWithSameOrigNameGroup = false;
            boolean isColocateTable = colocateTableIndex.isColocateTable(tabletMeta.getTableId());
            // chooseBackendsArbitrary is true, means this may be the first table of colocation group,
            // or this is just a normal table, and we can choose backends arbitrary.
            // otherwise, backends should be chosen from backendsPerBucketSeq;
            boolean chooseBackendsArbitrary;

            // We should synchronize the creation of colocate tables, otherwise it can have concurrent issues.
            // Considering the following situation,
            // T1: P1 issues `create colocate table` and finds that there isn't a bucket sequence associated
            //     with the colocate group, so it will initialize the bucket sequence for the first time
            // T2: P2 do the same thing as P1
            // T3: P1 set the bucket sequence for colocate group stored in `ColocateTableIndex`
            // T4: P2 also set the bucket sequence, hence overwrite what P1 just wrote
            // T5: After P1 creates the colocate table, the actual tablet distribution won't match the bucket sequence
            //     of the colocate group, and balancer will create a lot of COLOCATE_MISMATCH tasks which shouldn't exist.
            if (isColocateTable) {
                try {
                    // Optimization: wait first time, before global lock
                    colocateTableCreateSyncer.awaitZero();
                    // Since we have supported colocate tables in different databases,
                    // we should use global lock, not db lock.
                    GlobalStateMgr.getCurrentState().tryLock(false);
                    try {
                        // Wait again, for safety
                        // We are in global lock, we should have timeout in case holding lock for too long
                        colocateTableCreateSyncer.awaitZero(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS);
                        // if this is a colocate table, try to get backend seqs from colocation index.
                        groupId = colocateTableIndex.getGroup(tabletMeta.getTableId());
                        backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
                        if (backendsPerBucketSeq.isEmpty()) {
                            List<ColocateTableIndex.GroupId> colocateWithGroupsInOtherDb =
                                    colocateTableIndex.getColocateWithGroupsInOtherDb(groupId);
                            if (!colocateWithGroupsInOtherDb.isEmpty()) {
                                backendsPerBucketSeq =
                                        colocateTableIndex.getBackendsPerBucketSeq(colocateWithGroupsInOtherDb.get(0));
                                initBucketSeqWithSameOrigNameGroup = true;
                            }
                        }
                        chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();
                        if (chooseBackendsArbitrary) {
                            colocateTableCreateSyncer.increment();
                        }
                    } finally {
                        GlobalStateMgr.getCurrentState().unlock();
                    }
                } catch (InterruptedException e) {
                    LOG.warn("wait for concurrent colocate table creation finish failed, msg: {}",
                            e.getMessage(), e);
                    Thread.currentThread().interrupt();
                    throw new DdlException("wait for concurrent colocate table creation finish failed", e);
                }
            } else {
                chooseBackendsArbitrary = true;
            }

            try {
                if (chooseBackendsArbitrary) {
                    backendsPerBucketSeq = com.google.common.collect.Lists.newArrayList();
                }
                for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
                    // create a new tablet with random chosen backends
                    LocalTablet tablet = new LocalTablet(GlobalStateMgr.getCurrentState().getNextId());

                    // add tablet to inverted index first
                    materializedIndex.addTablet(tablet, tabletMeta);
                    tabletIdSet.add(tablet.getId());

                    // get BackendIds
                    List<Long> chosenBackendIds;
                    if (chooseBackendsArbitrary) {
                        // This is the first colocate table in the group, or just a normal table,
                        // randomly choose backends
                        if (Config.enable_strict_storage_medium_check) {
                            chosenBackendIds =
                                    chosenBackendIdBySeq(replicationNum, locReq, tabletMeta.getStorageMedium());
                        } else {
                            try {
                                chosenBackendIds = chosenBackendIdBySeq(replicationNum, locReq);
                            } catch (DdlException ex) {
                                throw new DdlException(String.format("%s, table=%s, default_replication_num=%d",
                                        ex.getMessage(), table.getName(), Config.default_replication_num));
                            }
                        }
                        backendsPerBucketSeq.add(chosenBackendIds);
                    } else {
                        // get backends from existing backend sequence
                        chosenBackendIds = backendsPerBucketSeq.get(i);
                    }

                    // create replicas
                    for (long backendId : chosenBackendIds) {
                        long replicaId = GlobalStateMgr.getCurrentState().getNextId();
                        Replica replica = new Replica(replicaId, backendId, Replica.ReplicaState.NORMAL, version,
                                tabletMeta.getOldSchemaHash());
                        tablet.addReplica(replica);
                    }
                    Preconditions.checkState(chosenBackendIds.size() == replicationNum,
                            chosenBackendIds.size() + " vs. " + replicationNum);
                }

                // In the following two situations, we should set the bucket seq for colocate group and persist the info,
                //   1. This is the first time we add a table to colocate group, and it doesn't have the same original name
                //      with colocate group in other database.
                //   2. It's indeed the first time, but it should colocate with group in other db
                //      (because of having the same original name), we should use the bucket
                //      seq of other group to initialize our own.
                if ((groupId != null && chooseBackendsArbitrary) || initBucketSeqWithSameOrigNameGroup) {
                    colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                    ColocatePersistInfo info =
                            ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                    GlobalStateMgr.getCurrentState().getEditLog().logColocateBackendsPerBucketSeq(info);
                }
            } finally {
                if (isColocateTable && chooseBackendsArbitrary) {
                    colocateTableCreateSyncer.decrement();
                }
            }
        }

        return partition;
    }

    private void updatePartitionInfo(PartitionInfo partitionInfo, List<Pair<Partition, PartitionDesc>> partitionList,
                                     Set<String> existPartitionNameSet, AddPartitionClause addPartitionClause,
                                     OlapTable olapTable)
            throws DdlException {
        boolean isTempPartition = addPartitionClause.isTempPartition();
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            rangePartitionInfo.handleNewRangePartitionDescs(partitionList, existPartitionNameSet, isTempPartition);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            listPartitionInfo.handleNewListPartitionDescs(partitionList, existPartitionNameSet, isTempPartition);
        } else {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }

        if (isTempPartition) {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addTempPartition(partition);
                }
            }
        } else {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addPartition(partition);
                }
            }
        }
    }

    private void addPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                 AddPartitionClause addPartitionClause, PartitionInfo partitionInfo,
                                 List<Partition> partitionList, Set<String> existPartitionNameSet)
            throws DdlException {
        PartitionType partitionType = partitionInfo.getType();
        if (partitionInfo.isRangePartition()) {
            addRangePartitionLog(db, olapTable, partitionDescs, addPartitionClause, partitionInfo, partitionList,
                    existPartitionNameSet);
        } else if (partitionType == PartitionType.LIST) {
            addListPartitionLog(db, olapTable, partitionDescs, addPartitionClause, partitionInfo, partitionList,
                    existPartitionNameSet);
        } else {
            throw new DdlException("Only support adding partition log to range/list partitioned table");
        }
    }


    private void addRangePartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                      AddPartitionClause addPartitionClause, PartitionInfo partitionInfo,
                                      List<Partition> partitionList, Set<String> existPartitionNameSet) {
        boolean isTempPartition = addPartitionClause.isTempPartition();
        int partitionLen = partitionList.size();
        List<PartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayListWithCapacity(partitionLen);
        if (partitionLen == 1) {
            Partition partition = partitionList.get(0);
            if (existPartitionNameSet.contains(partition.getName())) {
                LOG.info("add partition[{}] which already exists", partition.getName());
                return;
            }
            PartitionPersistInfoV2 info = new RangePartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                    partitionDescs.get(0).getPartitionDataProperty(),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()), isTempPartition,
                    ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                    ((SingleRangePartitionDesc) partitionDescs.get(0)).getDataCacheInfo());
            partitionInfoV2List.add(info);
            AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);

            LOG.info("succeed in creating partition[{}], name: {}, temp: {}", partition.getId(),
                    partition.getName(), isTempPartition);
        } else {
            for (int i = 0; i < partitionLen; i++) {
                Partition partition = partitionList.get(i);
                if (!existPartitionNameSet.contains(partition.getName())) {
                    PartitionPersistInfoV2 info = new RangePartitionPersistInfo(db.getId(), olapTable.getId(),
                            partition, partitionDescs.get(i).getPartitionDataProperty(),
                            partitionInfo.getReplicationNum(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()), isTempPartition,
                            ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                            ((SingleRangePartitionDesc) partitionDescs.get(i)).getDataCacheInfo());

                    partitionInfoV2List.add(info);
                }
            }

            AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);

            for (PartitionPersistInfoV2 infoV2 : partitionInfoV2List) {
                LOG.info("succeed in creating partition[{}], name: {}, temp: {}", infoV2.getPartition().getId(),
                        infoV2.getPartition().getName(), isTempPartition);
            }
        }
    }

    @VisibleForTesting
    public void addListPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                    AddPartitionClause addPartitionClause, PartitionInfo partitionInfo,
                                    List<Partition> partitionList, Set<String> existPartitionNameSet)
            throws DdlException {
        if (partitionList == null) {
            throw new DdlException("partitionList should not null");
        } else if (partitionList.size() == 0) {
            return;
        }

        // TODO: add only 1 log for multi list partition
        int i = 0;
        for (Partition partition : partitionList) {
            boolean isTempPartition = addPartitionClause.isTempPartition();
            if (existPartitionNameSet.contains(partition.getName())) {
                LOG.info("add partition[{}] which already exists", partition.getName());
                continue;
            }
            long partitionId = partition.getId();
            PartitionPersistInfoV2 info = new ListPartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                    partitionDescs.get(i).getPartitionDataProperty(),
                    partitionInfo.getReplicationNum(partitionId),
                    partitionInfo.getIsInMemory(partitionId),
                    isTempPartition,
                    ((ListPartitionInfo) partitionInfo).getIdToValues().get(partitionId),
                    ((ListPartitionInfo) partitionInfo).getIdToMultiValues().get(partitionId),
                    partitionDescs.get(i).getDataCacheInfo());
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartition(info);
            LOG.info("succeed in creating list partition[{}], name: {}, temp: {}", partitionId,
                    partition.getName(), isTempPartition);
            i++;
        }
    }

    private static OlapTable checkTable(Database db, String tableName) throws DdlException {
        CatalogUtils.checkTableExist(db, tableName);
        Table table = db.getTable(tableName);
        CatalogUtils.checkNativeTable(db, table);
        OlapTable olapTable = (OlapTable) table;
        CatalogUtils.checkTableState(olapTable, tableName);
        return olapTable;
    }


    // create replicas for tablet with random chosen backends
    private static List<Long> chosenBackendIdBySeq(int replicationNum, Multimap<String, String> locReq,
                                                   TStorageMedium storageMedium)
            throws DdlException {
        List<Long> chosenBackendIds =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getNodeSelector()
                        .seqChooseBackendIdsByStorageMedium(replicationNum,
                                true, true, locReq, storageMedium);
        if (CollectionUtils.isEmpty(chosenBackendIds)) {
            throw new DdlException(
                    "Failed to find enough hosts with storage medium " + storageMedium +
                            " at all backends, number of replicas needed: " +
                            replicationNum + ". Storage medium check failure can be forcefully ignored by executing " +
                            "'ADMIN SET FRONTEND CONFIG (\"enable_strict_storage_medium_check\" = \"false\");', " +
                            "but incompatible medium type can cause balance problem, so we strongly recommend" +
                            " creating table with compatible 'storage_medium' property set.");
        }
        return chosenBackendIds;
    }

    private static List<Long> chosenBackendIdBySeq(int replicationNum, Multimap<String, String> locReq) throws DdlException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> chosenBackendIds = systemInfoService.getNodeSelector()
                .seqChooseBackendIds(replicationNum, true, true, locReq);
        if (!CollectionUtils.isEmpty(chosenBackendIds)) {
            return chosenBackendIds;
        } else if (replicationNum > 1) {
            List<Long> backendIds = systemInfoService.getBackendIds(true);
            throw new DdlException(
                    String.format("Table replication num should be less than or equal to the number of available BE nodes. "
                            + "You can change this default by setting the replication_num table properties. "
                            + "Current alive backend is [%s]. ", Joiner.on(",").join(backendIds)));
        } else {
            throw new DdlException("No alive nodes");
        }
    }

    private void buildPartitions(long dbId,
                                 long tableId,
                                 Map<Long, MaterializedIndexMeta> indexMetaMap,
                                 List<PhysicalPartition> partitions,
                                 long warehouseId,
                                 PartitionDesc partitionDesc,
                                 boolean isCloudNativeTable,
                                 List<Index> indexes,
                                 Collection<String> bloomFilterColumnNames,
                                 double fpp,
                                 boolean enablePersistentIndex,
                                 TPersistentIndexType persistentIndexType,
                                 int primaryIndexCacheExpireSec,
                                 BinlogConfig binlogConfig,
                                 TCompressionType compressionType) {
        if (partitions.isEmpty()) {
            return;
        }
        int numAliveNodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();

        if (RunMode.isSharedDataMode()) {
            numAliveNodes = 0;
            List<Long> computeNodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
            for (long nodeId : computeNodeIds) {
                if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId).isAlive()) {
                    ++numAliveNodes;
                }
            }
        }
        if (numAliveNodes == 0) {
            if (RunMode.isSharedDataMode()) {
                throw new DdlException("no alive compute nodes");
            } else {
                throw new DdlException("no alive backends");
            }
        }

        int numReplicas = 0;
        for (PhysicalPartition partition : partitions) {
            numReplicas += partition.storageReplicaCount();
        }

        if (numReplicas > Config.create_table_max_serial_replicas) {
            LOG.info("start to build {} partitions concurrently for table {}.{} with {} replicas",
                    partitions.size(), db.getFullName(), table.getName(), numReplicas);
            buildPartitionsConcurrently(db.getId(), table, partitions, numReplicas, numAliveNodes, warehouseId, partitionInfo);
        } else {
            LOG.info("start to build {} partitions sequentially for table {}.{} with {} replicas",
                    partitions.size(), db.getFullName(), table.getName(), numReplicas);
            buildPartitionsSequentially(db.getId(), table, partitions, numReplicas, numAliveNodes, warehouseId, partitionInfo);
        }
    }

    private void buildPartitionsSequentially(long dbId,
                                             long tableId,
                                             Map<Long, MaterializedIndexMeta> indexMetaMap,
                                             List<PhysicalPartition> partitions,
                                             long warehouseId,
                                             PartitionDesc partitionDesc,
                                             boolean isCloudNativeTable,
                                             List<Index> indexes,
                                             Collection<String> bloomFilterColumnNames,
                                             double fpp,
                                             boolean enablePersistentIndex,
                                             TPersistentIndexType persistentIndexType,
                                             int primaryIndexCacheExpireSec,
                                             BinlogConfig binlogConfig,
                                             TCompressionType compressionType) throws DdlException {
        // Try to bundle at least 200 CreateReplicaTask's in a single AgentBatchTask.
        // The number 200 is just an experiment value that seems to work without obvious problems, feel free to
        // change it if you have a better choice.
        long start = System.currentTimeMillis();
        int avgReplicasPerPartition = numReplicas / partitions.size();
        int partitionGroupSize = Math.max(1, numBackends * 200 / Math.max(1, avgReplicasPerPartition));
        for (int i = 0; i < partitions.size(); i += partitionGroupSize) {
            int endIndex = Math.min(partitions.size(), i + partitionGroupSize);
            List<CreateReplicaTask> tasks = buildCreateReplicaTasks(
                    dbId,
                    tableId,
                    indexMetaMap.get(xxx),
                    partitions.get(xxx),
                    null,
                    warehouseId,
                    partitionDesc,
                    isCloudNativeTable,
                    indexes,
                    bloomFilterColumnNames,
                    fpp,
                    enablePersistentIndex,
                    persistentIndexType,
                    primaryIndexCacheExpireSec,
                    binlogConfig,
                    compressionType);
            int partitionCount = endIndex - i;
            int indexCountPerPartition = partitions.get(i).getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size();
            int timeout = Config.tablet_create_timeout_second * countMaxTasksPerBackend(tasks);
            // Compatible with older versions, `Config.max_create_table_timeout_second` is the timeout time for a single index.
            // Here we assume that all partitions have the same number of indexes.
            int maxTimeout = partitionCount * indexCountPerPartition * Config.max_create_table_timeout_second;
            try {
                LOG.info("build partitions sequentially, send task one by one, all tasks timeout {}s",
                        Math.min(timeout, maxTimeout));
                sendCreateReplicaTasksAndWaitForFinished(tasks, Math.min(timeout, maxTimeout));
                LOG.info("build partitions sequentially, all tasks finished, took {}ms",
                        System.currentTimeMillis() - start);
                tasks.clear();
            } finally {
                for (CreateReplicaTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
                }
            }
        }
    }

    private void buildPartitionsConcurrently(long dbId,
                                             long tableId,
                                             Map<Long, MaterializedIndexMeta> indexMetaMap,
                                             List<PhysicalPartition> partitions,
                                             long warehouseId,
                                             PartitionDesc partitionDesc,
                                             boolean isCloudNativeTable,
                                             List<Index> indexes,
                                             Collection<String> bloomFilterColumnNames,
                                             double fpp,
                                             boolean enablePersistentIndex,
                                             TPersistentIndexType persistentIndexType,
                                             int primaryIndexCacheExpireSec,
                                             BinlogConfig binlogConfig,
                                             TCompressionType compressionType) throws DdlException {
        long start = System.currentTimeMillis();
        int timeout = Math.max(1, numReplicas / numBackends) * Config.tablet_create_timeout_second;
        int numIndexes = partitions.stream().mapToInt(
                partition -> partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size()).sum();
        int maxTimeout = numIndexes * Config.max_create_table_timeout_second;
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(numReplicas);
        Map<Long, List<Long>> taskSignatures = new HashMap<>();
        try {
            int numFinishedTasks;
            int numSendedTasks = 0;
            long startTime = System.currentTimeMillis();
            long maxWaitTimeMs = Math.min(timeout, maxTimeout) * 1000L;
            for (PhysicalPartition partition : partitions) {
                if (!countDownLatch.getStatus().ok()) {
                    break;
                }
                List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partition, warehouseId);
                for (CreateReplicaTask task : tasks) {
                    List<Long> signatures =
                            taskSignatures.computeIfAbsent(task.getBackendId(), k -> new ArrayList<>());
                    signatures.add(task.getSignature());
                }
                sendCreateReplicaTasks(tasks, countDownLatch);
                numSendedTasks += tasks.size();
                numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                // Since there is no mechanism to cancel tasks, if we send a lot of tasks at once and some error or timeout
                // occurs in the middle of the process, it will create a lot of useless replicas that will be deleted soon and
                // waste machine resources. Sending a lot of tasks at once may also block other users' tasks for a long time.
                // To avoid these situations, new tasks are sent only when the average number of tasks on each node is less
                // than 200.
                // (numSendedTasks - numFinishedTasks) is number of tasks that have been sent but not yet finished.
                while (numSendedTasks - numFinishedTasks > 200 * numBackends) {
                    long currentTime = System.currentTimeMillis();
                    // Add timeout check
                    if (currentTime > startTime + maxWaitTimeMs) {
                        throw new TimeoutException("Wait in buildPartitionsConcurrently exceeded timeout");
                    }
                    ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
                    numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                }
            }
            LOG.info("build partitions concurrently for {}, waiting for all tasks finish with timeout {}s",
                    table.getName(), Math.min(timeout, maxTimeout));
            waitForFinished(countDownLatch, Math.min(timeout, maxTimeout));
            LOG.info("build partitions concurrently for {}, all tasks finished, took {}ms",
                    table.getName(), System.currentTimeMillis() - start);

        } catch (Exception e) {
            LOG.warn(e);
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw new DdlException(e.getMessage());
        } finally {
            if (!countDownLatch.getStatus().ok()) {
                for (Map.Entry<Long, List<Long>> entry : taskSignatures.entrySet()) {
                    for (Long signature : entry.getValue()) {
                        AgentTaskQueue.removeTask(entry.getKey(), TTaskType.CREATE, signature);
                    }
                }
            }
        }
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId,
                                                            long tableId,
                                                            MaterializedIndexMeta indexMeta,
                                                            PhysicalPartition partition,
                                                            MaterializedIndex index,
                                                            long warehouseId,
                                                            PartitionDesc partitionDesc,
                                                            boolean isCloudNativeTable,
                                                            List<Index> indexes,
                                                            Collection<String> bloomFilterColumnNames,
                                                            double fpp,
                                                            boolean enablePersistentIndex,
                                                            TPersistentIndexType persistentIndexType,
                                                            int primaryIndexCacheExpireSec,
                                                            BinlogConfig binlogConfig,
                                                            TCompressionType compressionType) {
        LOG.info("build create replica tasks for index {} db {} table {} partition {}",
                index, dbId, tableId, partition);
        boolean createSchemaFile = true;
        List<CreateReplicaTask> tasks = new ArrayList<>((int) index.getReplicaCount());
        TTabletType tabletType = isCloudNativeTable ? TTabletType.TABLET_TYPE_LAKE : TTabletType.TABLET_TYPE_DISK;
        TTabletSchema tabletSchema = SchemaInfo.newBuilder()
                .setId(indexMeta.getSchemaId())
                .setVersion(indexMeta.getSchemaVersion())
                .setKeysType(indexMeta.getKeysType())
                .setShortKeyColumnCount(indexMeta.getShortKeyColumnCount())
                .setSchemaHash(indexMeta.getSchemaHash())
                .setStorageType(indexMeta.getStorageType())
                .setIndexes(indexes)
                .setSortKeyIndexes(indexMeta.getSortKeyIdxes())
                .setSortKeyUniqueIds(indexMeta.getSortKeyUniqueIds())
                .setBloomFilterColumnNames(bloomFilterColumnNames)
                .setBloomFilterFpp(fpp)
                .addColumns(indexMeta.getSchema())
                .build().toTabletSchema();

        for (Tablet tablet : index.getTablets()) {
            List<Long> nodeIdsOfReplicas = new ArrayList<>();
            if (isCloudNativeTable) {
                long nodeId = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) tablet).getId();

                nodeIdsOfReplicas.add(nodeId);
            } else {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    nodeIdsOfReplicas.add(replica.getBackendId());
                }
            }

            for (Long nodeId : nodeIdsOfReplicas) {
                CreateReplicaTask task = CreateReplicaTask.newBuilder()
                        .setNodeId(nodeId)
                        .setDbId(dbId)
                        .setTableId(tableId)
                        .setPartitionId(partition.getId())
                        .setIndexId(index.getId())
                        .setTabletId(tablet.getId())
                        .setVersion(partition.getVisibleVersion())
                        .setStorageMedium(partitionDesc.getPartitionDataProperty().getStorageMedium())
                        .setEnablePersistentIndex(enablePersistentIndex)
                        .setPersistentIndexType(persistentIndexType)
                        .setPrimaryIndexCacheExpireSec(primaryIndexCacheExpireSec)
                        .setBinlogConfig(binlogConfig)
                        .setTabletType(tabletType)
                        .setCompressionType(compressionType)
                        .setTabletSchema(tabletSchema)
                        .setCreateSchemaFile(createSchemaFile)
                        .build();
                tasks.add(task);
                createSchemaFile = false;
            }
        }
        return tasks;
    }

    private int countMaxTasksPerBackend(List<CreateReplicaTask> tasks) {
        Map<Long, Integer> tasksPerBackend = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            tasksPerBackend.compute(task.getBackendId(), (k, v) -> (v == null) ? 1 : v + 1);
        }
        return Collections.max(tasksPerBackend.values());
    }

    // NOTE: Unfinished tasks will NOT be removed from the AgentTaskQueue.
    private void sendCreateReplicaTasksAndWaitForFinished(List<CreateReplicaTask> tasks, long timeout)
            throws DdlException {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(tasks.size());
        sendCreateReplicaTasks(tasks, countDownLatch);
        waitForFinished(countDownLatch, timeout);
    }

    private void sendCreateReplicaTasks(List<CreateReplicaTask> tasks,
                                        MarkedCountDownLatch<Long, Long> countDownLatch) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            task.setLatch(countDownLatch);
            countDownLatch.addMark(task.getBackendId(), task.getTabletId());
            AgentBatchTask batchTask = batchTaskMap.get(task.getBackendId());
            if (batchTask == null) {
                batchTask = new AgentBatchTask();
                batchTaskMap.put(task.getBackendId(), batchTask);
            }
            batchTask.addTask(task);
        }
        for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
            AgentTaskQueue.addBatchTask(entry.getValue());
            AgentTaskExecutor.submit(entry.getValue());
        }
    }

    // REQUIRE: must set countDownLatch to error stat before throw an exception.
    private void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) throws DdlException {
        try {
            if (countDownLatch.await(timeout, TimeUnit.SECONDS)) {
                if (!countDownLatch.getStatus().ok()) {
                    String errMsg = "fail to create tablet: " + countDownLatch.getStatus().getErrorMsg();
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } else { // timed out
                List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Map.Entry<Long, Long>> firstThree =
                        unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                StringBuilder sb = new StringBuilder("Table creation timed out. unfinished replicas");
                sb.append("(").append(firstThree.size()).append("/").append(unfinishedMarks.size()).append("): ");
                // Show details of the first 3 unfinished tablets.
                for (Map.Entry<Long, Long> mark : firstThree) {
                    sb.append(mark.getValue()); // TabletId
                    sb.append('(');
                    Backend backend = stateMgr.getNodeMgr().getClusterInfo().getBackend(mark.getKey());
                    sb.append(backend != null ? backend.getHost() : "N/A");
                    sb.append(") ");
                }
                sb.append(" timeout=").append(timeout).append('s');
                String errMsg = sb.toString();
                LOG.warn(errMsg);

                String userErrorMsg = String.format(
                        errMsg + "\n You can increase the timeout by increasing the " +
                                "config \"tablet_create_timeout_second\" and try again.\n" +
                                "To increase the config \"tablet_create_timeout_second\" (currently %d), " +
                                "run the following command:\n" +
                                "```\nadmin set frontend config(\"tablet_create_timeout_second\"=\"%d\")\n```\n" +
                                "or add the following configuration to the fe.conf file and restart the process:\n" +
                                "```\ntablet_create_timeout_second=%d\n```",
                        Config.tablet_create_timeout_second,
                        Config.tablet_create_timeout_second * 2,
                        Config.tablet_create_timeout_second * 2
                );
                countDownLatch.countDownToZero(new Status(TStatusCode.TIMEOUT, "timed out"));
                throw new DdlException(userErrorMsg);
            }
        } catch (InterruptedException e) {
            LOG.warn(e);
            countDownLatch.countDownToZero(new Status(TStatusCode.CANCELLED, "cancelled"));
        }
    }
}

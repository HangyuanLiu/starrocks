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

package com.starrocks.lake;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.backup.Status;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata for StarRocks lake table
 * todo: Rename to CloudNativeTable
 */
public class LakeTable extends OlapTable {

    private static final Logger LOG = LogManager.getLogger(LakeTable.class);

    public LakeTable() {
        super(TableType.CLOUD_NATIVE);
    }

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo, TableIndexes indexes) {
        super(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, indexes, TableType.CLOUD_NATIVE);
    }

    public LakeTable(long id, String tableName, List<Column> baseSchema, KeysType keysType, PartitionInfo partitionInfo,
                     DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    @Override
    public FileCacheInfo getPartitionFileCacheInfo(long partitionId) {
        FileCacheInfo cacheInfo = null;
        DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        if (dataCacheInfo == null) {
            cacheInfo = tableProperty.getStorageInfo().getCacheInfo();
        } else {
            cacheInfo = dataCacheInfo.getCacheInfo();
        }
        return cacheInfo;
    }

    @Override
    public void setStorageInfo(FilePathInfo pathInfo, DataCacheInfo dataCacheInfo) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.setStorageInfo(new StorageInfo(pathInfo, dataCacheInfo.getCacheInfo()));
    }

    @Override
    public OlapTable selectiveCopy(Collection<String> reservedPartitions, boolean resetState,
                                   MaterializedIndex.IndexExtState extState) {
        LakeTable copied = DeepCopy.copyWithGson(this, LakeTable.class);
        if (copied == null) {
            LOG.warn("failed to copy lake table: {}", getName());
            return null;
        }
        return selectiveCopyInternal(copied, reservedPartitions, resetState, extState);
    }

    public static LakeTable read(DataInput in) throws IOException {
        // type is already read in Table
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeTable.class);
    }

    @Override
    public boolean isDeleteRetryable() {
        return true;
    }

    @Override
    public boolean delete(long dbId, boolean replay) {
        return LakeTableHelper.deleteTable(dbId, this, replay);
    }

    @Override
    public boolean deleteFromRecycleBin(long dbId, boolean replay) {
        return LakeTableHelper.deleteTableFromRecycleBin(dbId, this, replay);
    }

    @Override
    public AlterJobV2Builder alterTable() {
        return LakeTableHelper.alterTable(this);
    }

    @Override
    public AlterJobV2Builder rollUp() {
        return LakeTableHelper.rollUp(this);
    }

    @Override
    public Map<String, String> getUniqueProperties() {
        Map<String, String> properties = Maps.newHashMap();

        if (tableProperty != null) {
            StorageInfo storageInfo = tableProperty.getStorageInfo();
            if (storageInfo != null) {
                // datacache.enable
                properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE,
                        String.valueOf(storageInfo.isEnableDataCache()));

                // enable_async_write_back
                properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK,
                        String.valueOf(storageInfo.isEnableAsyncWriteBack()));
            }

            // datacache partition duration
            String partitionDuration =
                    tableProperty.getProperties().get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION);
            if (partitionDuration != null) {
                properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, partitionDuration);
            }
        }

        // storage volume
        StorageVolumeMgr svm = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME, svm.getStorageVolumeNameOfTable(id));

        // persistent index type
        if (keysType == KeysType.PRIMARY_KEYS && enablePersistentIndex()
                && !Strings.isNullOrEmpty(getPersistentIndexTypeString())) {
            properties.put(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, getPersistentIndexTypeString());
        }

        return properties;
    }

    @Override
    public Status createTabletsForRestore(int tabletNum, MaterializedIndex index, GlobalStateMgr globalStateMgr,
                                          int replicationNum, long version, int schemaHash,
                                          long physicalPartitionId, Database db) {
        FilePathInfo fsInfo = getPartitionFilePathInfo(physicalPartitionId);
        FileCacheInfo cacheInfo = getPartitionFileCacheInfo(physicalPartitionId);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
        List<Long> shardIds = null;
        try {
            // Ignore the parameter replicationNum
            shardIds = globalStateMgr.getStarOSAgent().createShards(tabletNum, fsInfo, cacheInfo, index.getShardGroupId(),
                    null, properties, WarehouseManager.DEFAULT_RESOURCE);
        } catch (DdlException e) {
            LOG.error(e.getMessage(), e);
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        for (long shardId : shardIds) {
            LakeTablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, null /* tablet meta */, false/* update inverted index */);
        }
        return Status.OK;
    }

    // used in colocate table index, return an empty list for LakeTable
    @Override
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        return Lists.newArrayList();
    }

    public List<Long> getShardGroupIds() {
        List<Long> shardGroupIds = new ArrayList<>();
        for (Partition p : getAllPartitions()) {
            for (MaterializedIndex index : p.getDefaultPhysicalPartition()
                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                shardGroupIds.add(index.getShardGroupId());
            }
        }
        return shardGroupIds;
    }

    @Override
    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return TableType.OLAP.name();
    }

    @Override
    public String getDisplayComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return CatalogUtils.addEscapeCharacter(comment);
        }
        return TableType.OLAP.name();
    }

    @Override
    protected RecyclePartitionInfo buildRecyclePartitionInfo(long dbId, Partition partition) {
        if (partitionInfo.isRangePartition()) {
            Range<PartitionKey> range = ((RangePartitionInfo) partitionInfo).getRange(partition.getId());
            return new RecycleLakeRangePartitionInfo(dbId, id, partition, range,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else if (partitionInfo.isListPartition()) {
            return new RecycleLakeListPartitionInfo(dbId, id, partition,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else if (partitionInfo.isUnPartitioned()) {
            return new RecycleLakeUnPartitionInfo(dbId, id, partition,
                    partitionInfo.getDataProperty(partition.getId()),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()),
                    partitionInfo.getDataCacheInfo(partition.getId()));
        } else {
            throw new RuntimeException("Unknown partition type: " + partitionInfo.getType());
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // We should restore column unique id before calling super.gsonPostProcess(), which will rebuild full schema there.
        // And the max unique id will be reset while rebuilding full schema.
        LakeTableHelper.restoreColumnUniqueIdIfNeeded(this);
        super.gsonPostProcess();

        LakeTable.Branch mainBranch = new LakeTable.Branch();
        nameToBranch.put("main", mainBranch);
        for (PhysicalPartition physicalPartition : getAllPhysicalPartitions()) {
            mainBranch.partitionIdToVersion.putIfAbsent(physicalPartition.getId(), new LakeTable.PartitionVersion());
            LakeTable.PartitionVersion partitionVersion =
                    mainBranch.partitionIdToVersion.get(physicalPartition.getId());
            partitionVersion.addVersion(physicalPartition.getVisibleVersionTime(), physicalPartition.getVisibleVersion());
        }
    }

    @Override
    public boolean getUseFastSchemaEvolution() {
        return !hasRowStorageType() && Config.enable_fast_schema_evolution_in_share_data_mode;
    }

    @Override
    public void checkStableAndNormal() throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(state, getName());
        }
    }

    public boolean checkLakeRollupAllowFileBundling() {
        return getPartitions().stream()
                .flatMap(partition -> partition.getSubPartitions().stream())
                .allMatch(physicalPartition -> {
                    long physicalPartitionId = physicalPartition.getId();
                    List<Long> shardIds = physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).stream()
                            .flatMap(index -> index.getTablets().stream())
                            .map(tablet -> ((LakeTablet) tablet).getShardId())
                            .collect(Collectors.toList());

                    if (shardIds.isEmpty()) {
                        return true;
                    }
                    List<ShardInfo> shardInfos = new ArrayList<>();
                    try {
                        shardInfos = GlobalStateMgr.getCurrentState().getStarOSAgent()
                                .getShardInfo(shardIds, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                    } catch (Exception e) {
                        LOG.warn("checkLakeRollupAllowFileBundling got exception: {}", e.getMessage());
                        return false;
                    }
                    return shardInfos.stream()
                            .allMatch(shardInfo -> LakeTableHelper.extractIdFromPath(shardInfo.getFilePath().getFullPath())
                                    .map(id -> id == physicalPartitionId)
                                    .orElse(false));
                });
    }

    private Map<String, Branch> nameToBranch = new HashMap<>();
    private Map<String, Tag> nameToTag = new HashMap<>();

    public static class Branch {
        Map<Long, PartitionVersion> partitionIdToVersion = new HashMap<>();

        @Override
        public Object clone() {
            Branch branch = new Branch();

            for (Map.Entry<Long, PartitionVersion> entry : partitionIdToVersion.entrySet()) {
                PartitionVersion thisPartitionVersion = entry.getValue();

                PartitionVersion newPartitionVersion = new PartitionVersion();
                newPartitionVersion.gtidToVersion = new HashMap<>(thisPartitionVersion.gtidToVersion);
                branch.partitionIdToVersion.put(entry.getKey(), newPartitionVersion);
            }

            return branch;
        }
    }

    public static class Tag {
        private final Long gtid;
        private final String tagName;
        private final String branchName;

        public Tag(Long gtid, String tagName, String branchName) {
            this.gtid = gtid;
            this.tagName = tagName;
            this.branchName = branchName;
        }

        public Long getGtid() {
            return gtid;
        }

        public String getTagName() {
            return tagName;
        }

        public String getBranchName() {
            return branchName;
        }
    }

    static class PartitionVersion {
        Map<Long, Long> gtidToVersion = new HashMap<>();

        public void addVersion(Long timestamp, Long version) {
            gtidToVersion.put(timestamp, version);
        }
    }

    public Branch getBranch(long tableId, String branchName) {
        Branch branch = nameToBranch.get(branchName);
        return branch;
    }

    public void addBranch(long tableId, String branchName, Branch branch) {
        nameToBranch.put(branchName, branch);
    }

    public void addTag(long gtid, String tagName, String branchName) {
        Tag tag = new Tag(gtid, tagName, branchName);
        nameToTag.put(tagName, tag);
    }

    public Tag getTag(String tagName) {
        return nameToTag.get(tagName);
    }

    public void addVersion(String branchName,
                           Long physicalPartitionId,
                           Long gtid,
                           Long version) {
        if (branchName == null) {
            branchName = "main";
        }

        Branch branch = nameToBranch.get(branchName);

        branch.partitionIdToVersion.putIfAbsent(physicalPartitionId, new PartitionVersion());
        PartitionVersion partitionVersion = branch.partitionIdToVersion.get(physicalPartitionId);
        partitionVersion.addVersion(gtid, version);
    }

    public Long getVersion(String branchName,
                           Long physicalPartitionId,
                           Long gtid) {
        if (branchName == null) {
            branchName = "main";
        }

        Branch branch = nameToBranch.get(branchName);


        PartitionVersion partitionVersion = branch.partitionIdToVersion.get(physicalPartitionId);

        long finalGtid = 0;
        for (Map.Entry<Long, Long> entry : partitionVersion.gtidToVersion.entrySet()) {
            if (entry.getKey() < gtid) {
                if (entry.getKey() > finalGtid) {
                    finalGtid = entry.getKey();
                }
            }
        }

        if (partitionVersion.gtidToVersion.containsKey(finalGtid)) {
            return partitionVersion.gtidToVersion.get(finalGtid);
        } else {
            PhysicalPartition partition = getPhysicalPartition(physicalPartitionId);
            return partition.getVisibleVersion();
        }
    }

    public long getMinVersion(Long physicalPartitionId) {
        long version = Long.MAX_VALUE;
        for (Branch branch : nameToBranch.values()) {
            branch.partitionIdToVersion.putIfAbsent(physicalPartitionId, new PartitionVersion());

            PartitionVersion partitionVersion = branch.partitionIdToVersion.get(physicalPartitionId);
            long pv = retainTop5MaxKeys(partitionVersion.gtidToVersion);

            if (pv < version) {
                version = pv;
            }
        }

        /*
        for (TableVersion tableVersion : snapshot.tagToTableVersionMap.values()) {
            tableVersion.partitionIdToVersion.putIfAbsent(physicalPartitionId, new PartitionVersion());

            PartitionVersion partitionVersion = tableVersion.partitionIdToVersion.get(physicalPartitionId);
            long pv = retainTop5MaxKeys(partitionVersion.timestampToVersion);

            if (pv < version) {
                version = pv;
            }
        }

         */

        if (version < Long.MAX_VALUE) {
            return version;
        } else {
            PhysicalPartition physicalPartition = getPhysicalPartition(physicalPartitionId);
            return physicalPartition.getVisibleVersion();
        }
    }

    public Long retainTop5MaxKeys(Map<Long, Long> map) {
        if (map.isEmpty()) {
            return Long.MAX_VALUE;
        }

        int limit = Math.min(map.size(), 5);

        // 获取按照key降序排序的前5个entry
        List<Map.Entry<Long, Long>> top5Entries = map.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByKey().reversed())
                .limit(limit)
                .collect(Collectors.toList());

        return top5Entries.get(limit - 1).getValue();
    }

    @Override
    public void copyOnlyForQuery(OlapTable olapTable) {
        super.copyOnlyForQuery(olapTable);
        LakeTable lakeTable = (LakeTable) olapTable;
        lakeTable.nameToTag = this.nameToTag;
        lakeTable.nameToBranch = this.nameToBranch;
    }
}

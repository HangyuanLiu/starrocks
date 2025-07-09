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

package com.starrocks.catalog.branching;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.lake.LakeTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaSnapshot {
    //private final Long schemaId;
    private final Map<Long, Long> materializedIndexIdToSchemaId;
    // partition id -> partition snapshot id
    private final Long partitionSchemaId;
    //private final Map<Long, Long> partitionPhysicalSchemaId;
    private final TabletSnapshot tabletSnapshot;

    public SchemaSnapshot(Map<Long, Long> materializedIndexIdToSchemaId, Long partitionSchemaId, TabletSnapshot tabletSnapshot) {
        this.materializedIndexIdToSchemaId = materializedIndexIdToSchemaId;
        this.partitionSchemaId = partitionSchemaId;
        this.tabletSnapshot = tabletSnapshot;
    }

    public Map<Long, Long> getMaterializedIndexIdToSchemaId() {
        return materializedIndexIdToSchemaId;
    }

    public Long getSchemaId(Long materializedIndexId) {
        return materializedIndexIdToSchemaId.get(materializedIndexId);
    }

    public Long getPartitionSchemaId() {
        return partitionSchemaId;
    }

    public TabletSnapshot getTabletSnapshot() {
        return tabletSnapshot;
    }

    public static class TabletSnapshot {
        public final Map<Long, List<Long>> partitionToPhysicalPartitionId;
        public final Map<Long, Long> physicalPartitionIdToMaterializedIndexId;

        public TabletSnapshot(Map<Long, List<Long>> partitionToPhysicalPartitionId,
                              Map<Long, Long> physicalPartitionIdToMaterializedIndexId) {
            this.partitionToPhysicalPartitionId = partitionToPhysicalPartitionId;
            this.physicalPartitionIdToMaterializedIndexId = physicalPartitionIdToMaterializedIndexId;
        }
    }

    public static void create(OlapTable table) {
        //FIXME: use schema change transaction id
        Long gtid = GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid();

        Long partitionSchemaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setPartitionSchemaId(partitionSchemaId);
        table.addPartitionInfo(partitionSchemaId, (PartitionInfo) table.getPartitionInfo().clone());

        Map<Long, Long> materializedIndexIdToSchemaId = new HashMap<>();
        for (MaterializedIndexMeta indexMeta : table.getIndexIdToMeta().values()) {
            long materializedIndexId = indexMeta.getIndexId();
            long schemaId = indexMeta.getSchemaId();
            materializedIndexIdToSchemaId.put(materializedIndexId, schemaId);

            table.addSchema(schemaId, indexMeta.shallowCopy());
        }

        // build schema snapshot
        Map<Long, List<Long>> partitionToPhysicalPartitionId = new HashMap<>();
        Map<Long, Long> physicalPartitionIdToMaterializedIndexId = new HashMap<>();
        for (Partition partition : table.getPartitions()) {
            List<Long> physicalPartitionIdList = partition.getSubPartitions().stream().map(PhysicalPartition::getId)
                    .collect(Collectors.toList());
            partitionToPhysicalPartitionId.put(partition.getId(), physicalPartitionIdList);

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long physicalPartitionId = physicalPartition.getId();
                long materializedIndexId = physicalPartition.getBaseIndex().getId();
                physicalPartitionIdToMaterializedIndexId.put(physicalPartitionId, materializedIndexId);
            }
        }

        SchemaSnapshot snapshot = new SchemaSnapshot(materializedIndexIdToSchemaId, partitionSchemaId,
                new SchemaSnapshot.TabletSnapshot(partitionToPhysicalPartitionId, physicalPartitionIdToMaterializedIndexId));

        table.addSchemaSnapshot(gtid, snapshot);
    }

    public static void initMainBranch(LakeTable lakeTable) {
        LakeTable.Branch mainBranch = new LakeTable.Branch();
        lakeTable.addBranch(lakeTable.getId(), "main", mainBranch);
        for (PhysicalPartition physicalPartition : lakeTable.getAllPhysicalPartitions()) {
            lakeTable.addVersion("main",
                    physicalPartition.getId(),
                    physicalPartition.getVisibleVersionTime(),
                    physicalPartition.getVisibleVersion());
        }
    }

    public static void init() {
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        for (Database database : localMetastore.getAllDbs()) {
            for (Table table : database.getTables()) {
                if (table instanceof LakeTable lakeTable) {
                    if (lakeTable.getSchemaSnapshotHistory().isEmpty()) {
                        create(lakeTable);

                        initMainBranch(lakeTable);
                    }
                }
            }
        }
    }
}

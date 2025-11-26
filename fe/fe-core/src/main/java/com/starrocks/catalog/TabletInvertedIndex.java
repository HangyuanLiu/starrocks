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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TabletInvertedIndex.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.ConcurrentLong2ObjectHashMap;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/*
 * this class stores an inverted index
 * key is tablet id. value is the related ids of this tablet
 * Checkpoint thread is no need to modify this inverted index, because this inverted index will not be written
 * into image, all metadata are in globalStateMgr, and the inverted index will be rebuilt when FE restart.
 */
public class TabletInvertedIndex implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

    public static final TabletMeta NOT_EXIST_TABLET_META = new TabletMeta(NOT_EXIST_VALUE, NOT_EXIST_VALUE,
            NOT_EXIST_VALUE, NOT_EXIST_VALUE, TStorageMedium.HDD);

    // tablet id -> tablet meta
    private final ConcurrentLong2ObjectHashMap<TabletMeta> tabletMetaMap = new ConcurrentLong2ObjectHashMap<>();

    // tablet id -> replicas
    private final ConcurrentLong2ObjectHashMap<CopyOnWriteArrayList<Replica>> replicaMap =
            new ConcurrentLong2ObjectHashMap<>();

    // backend id -> tablet id list
    private final ConcurrentLong2ObjectHashMap<CopyOnWriteArrayList<Long>> backingReplicaMetaTable =
            new ConcurrentLong2ObjectHashMap<>();

    public TabletInvertedIndex() {
    }

    private CopyOnWriteArrayList<Replica> getOrCreateReplicaList(long tabletId) {
        CopyOnWriteArrayList<Replica> replicas = replicaMap.get(tabletId);
        if (replicas != null) {
            return replicas;
        }
        return replicaMap.computeIfAbsent(tabletId, k -> new CopyOnWriteArrayList<>());
    }

    private CopyOnWriteArrayList<Long> getOrCreateBackendTabletList(long backendId) {
        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        if (tabletIds != null) {
            return tabletIds;
        }
        return backingReplicaMetaTable.computeIfAbsent(backendId, k -> new CopyOnWriteArrayList<>());
    }

    private void removeTabletFromBackend(long backendId, long tabletId) {
        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        if (tabletIds == null) {
            return;
        }
        tabletIds.remove(tabletId);
    }

    public TabletMeta getTabletMeta(long tabletId) {
        return tabletMetaMap.get(tabletId);
    }

    public List<TabletMeta> getTabletMetaList(List<Long> tabletIdList) {
        List<TabletMeta> tabletMetaList = new ArrayList<>(tabletIdList.size());
        for (Long tabletId : tabletIdList) {
            tabletMetaList.add(tabletMetaMap.getOrDefault(tabletId, NOT_EXIST_TABLET_META));
        }
        return tabletMetaList;
    }

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        tabletMetaMap.putIfAbsent(tabletId, tabletMeta);
        LOG.debug("add tablet: {} tabletMeta: {}", tabletId, tabletMeta);
    }

    public void deleteTablet(long tabletId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        GlobalStateMgr.getCurrentState().getForceDeleteTracker().eraseTablet(tabletId);

        tabletMetaMap.remove(tabletId);

        CopyOnWriteArrayList<Replica> replicas = replicaMap.remove(tabletId);
        if (replicas != null) {
            for (Replica replica : replicas) {
                removeTabletFromBackend(replica.getBackendId(), tabletId);
            }
        }

        LOG.debug("delete tablet: {}", tabletId);
    }

    // Only for test
    public Map<Long, Map<Long, Replica>> getReplicaMetaTable() {
        //return replicaMetaTable;
        return null;
    }

    public void addReplica(long tabletId, Replica replica) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
        Preconditions.checkState(tabletMeta != null, "tablet %s does not exist", tabletId);
        CopyOnWriteArrayList<Replica> replicas = getOrCreateReplicaList(tabletId);
        replicas.removeIf(existing -> existing.getBackendId() == replica.getBackendId());
        replicas.add(replica);
        getOrCreateBackendTabletList(replica.getBackendId()).addIfAbsent(tabletId);

        LOG.debug("add replica {} of tablet {} in backend {}",
                replica.getId(), tabletId, replica.getBackendId());
    }

    public void deleteReplica(long tabletId, long backendId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        if (tabletMetaMap.get(tabletId) == null) {
            return;
        }
        CopyOnWriteArrayList<Replica> replicaList = replicaMap.get(tabletId);
        if (replicaList == null) {
            return;
        }
        boolean removed = replicaList.removeIf(replica -> replica.getBackendId() == backendId);
        if (!removed) {
            return;
        }
        removeTabletFromBackend(backendId, tabletId);
    }

    public Replica getReplica(long tabletId, long backendId) {
        CopyOnWriteArrayList<Replica> replicas = replicaMap.get(tabletId);
        if (replicas == null) {
            return null;
        }
        for (Replica replica : replicas) {
            if (replica.getBackendId() == backendId) {
                return replica;
            }
        }
        return null;
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        CopyOnWriteArrayList<Replica> replicas = replicaMap.get(tabletId);
        if (replicas == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(replicas);
    }

    /**
     * For each tabletId in the tablet_id list, get the replica on specified backend or null, return as a list.
     *
     * @param tabletIds tablet_id list
     * @param backendId backendid
     * @return list of replica or null if backend not found
     */
    public List<Replica> getReplicasOnBackendByTabletIds(List<Long> tabletIds, long backendId) {
        List<Replica> replicas = Lists.newArrayListWithCapacity(tabletIds.size());
        boolean hasReplica = false;
        for (Long tabletId : tabletIds) {
            Replica replica = getReplica(tabletId, backendId);
            if (replica != null) {
                hasReplica = true;
            }
            replicas.add(replica);
        }
        if (!hasReplica) {
            CopyOnWriteArrayList<Long> backendTabletIds = backingReplicaMetaTable.get(backendId);
            if (backendTabletIds == null || backendTabletIds.isEmpty()) {
                return null;
            }
        }
        return replicas;
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        if (tabletIds == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(tabletIds);
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        List<Long> result = Lists.newArrayList();

        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        if (tabletIds == null) {
            return result;
        }
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
            if (tabletMeta == null) {
                continue;
            }
            if (tabletMeta.getStorageMedium() == storageMedium) {
                result.add(tabletId);
            }
        }
        return result;
    }

    public long getTabletNumByBackendId(long backendId) {
        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        return tabletIds == null ? 0 : tabletIds.size();
    }

    /**
     * Get the number of tablets on the specified backend, grouped by pathHash
     *
     * @param backendId the ID of the backend
     * @return Map<pathHash, tabletNum> the number of tablets grouped by pathHash
     * @implNote Linear scan, invoke this interface with caution if the number of replicas is large
     */
    public Map<Long, Long> getTabletNumByBackendIdGroupByPathHash(long backendId) {
        Map<Long, Long> pathHashToTabletNum = Maps.newHashMap();

        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        if (tabletIds == null) {
            return pathHashToTabletNum;
        }
        for (Long tabletId : tabletIds) {
            CopyOnWriteArrayList<Replica> replicas = replicaMap.get(tabletId);
            if (replicas == null) {
                continue;
            }
            for (Replica r : replicas) {
                pathHashToTabletNum.compute(r.getPathHash(), (k, v) -> v == null ? 1L : v + 1);
            }
        }

        return pathHashToTabletNum;
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;
        CopyOnWriteArrayList<Long> tabletIds = backingReplicaMetaTable.get(backendId);
        if (tabletIds == null) {
            replicaNumMap.put(TStorageMedium.HDD, hddNum);
            replicaNumMap.put(TStorageMedium.SSD, ssdNum);
            return replicaNumMap;
        }
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
            if (tabletMeta == null) {
                continue;
            }
            if (tabletMeta.getStorageMedium() == TStorageMedium.HDD) {
                hddNum++;
            } else {
                ssdNum++;
            }
        }
        replicaNumMap.put(TStorageMedium.HDD, hddNum);
        replicaNumMap.put(TStorageMedium.SSD, ssdNum);
        return replicaNumMap;
    }

    public long getTabletCount() {
        return this.tabletMetaMap.size();
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (CopyOnWriteArrayList<Replica> replicas : replicaMap.values()) {
            replicaCount += replicas.size();
        }
        return replicaCount;
    }

    // just for test
    public void clear() {
        tabletMetaMap.clear();
        replicaMap.clear();
        backingReplicaMetaTable.clear();
        GlobalStateMgr.getCurrentState().getForceDeleteTracker().clear();
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("TabletMeta", getTabletCount(),
                "TabletCount", getTabletCount(),
                "ReplicateCount", getReplicaCount());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> tabletMetaSamples = tabletMetaMap.values()
                .stream()
                .limit(1)
                .collect(Collectors.toList());

        return Lists.newArrayList(Pair.create(tabletMetaSamples, (long) tabletMetaMap.size()));
    }
}

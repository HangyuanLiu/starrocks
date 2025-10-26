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

import com.google.common.collect.Maps;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.RendezvousHashRing;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.CandidateWorkerProvider;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Backend selector for StarRocks table (External Catalog).
 * <p>
 * Assign scan ranges to backend using consistent hashing based on Tablet ID (RPC mode)
 * or File Path (Direct Access mode).
 */
public class StarRocksBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(StarRocksBackendSelector.class);

    // be -> assigned bytes
    Map<ComputeNode, Long> assignedBytesPerComputeNode = Maps.newHashMap();
    // be -> re-balanced bytes
    Map<ComputeNode, Long> reBalancedBytesPerComputeNode = Maps.newHashMap();
    // be -> assigned scan ranges
    Map<ComputeNode, Long> assignedScanRangesPerComputeNode = Maps.newHashMap();

    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final WorkerProvider workerProvider;
    private final WorkerProvider candidateWorkerProvider;
    private final ConnectContext connectContext;
    private final boolean shuffleScanRange;
    private final boolean useIncrementalScanRanges;
    private final int kCandidateNumber = 3;
    // After testing, this value can ensure that the scan range size assigned to each BE is as uniform as possible,
    // and the largest scan data is not more than 1.1 times of the average value
    private final double kMaxImbalanceRatio = 1.1;

    class StarRocksScanRangeHasher {
        public void acceptScanRangeLocations(TScanRangeLocations tScanRangeLocations, PrimitiveSink primitiveSink) {
            if (tScanRangeLocations.scan_range.isSetInternal_scan_range()) {
                // For StarRocksScanNode, use tablet_id for hashing to ensure data cache affinity
                // TODO(harbor): In the future, we should use Source-Grouped Hashing to reduce RPC connections
                primitiveSink.putLong(tScanRangeLocations.scan_range.getInternal_scan_range().getTablet_id());
            } else {
                // Fallback for unexpected cases, though StarRocksScanNode should currently always have internal_scan_range
                primitiveSink.putInt(tScanRangeLocations.hashCode());
            }
        }
    }

    private final StarRocksScanRangeHasher starRocksScanRangeHasher;

    public StarRocksBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                    FragmentScanRangeAssignment assignment, WorkerProvider workerProvider,
                                    boolean shuffleScanRange,
                                    boolean useIncrementalScanRanges,
                                    ConnectContext connectContext) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
        this.connectContext = connectContext;
        this.starRocksScanRangeHasher = new StarRocksScanRangeHasher();
        this.shuffleScanRange = shuffleScanRange;
        this.useIncrementalScanRanges = useIncrementalScanRanges;
        this.candidateWorkerProvider = initCandidateWorkerProvider();
    }

    private WorkerProvider initCandidateWorkerProvider() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isEnableDataCacheSharing() ||
                isCacheSharingExpired(sessionVariable.getDataCacheSharingWorkPeriod())) {
            return null;
        }

        WorkerProvider.Factory factory = new CandidateWorkerProvider.Factory();
        WorkerProvider candidateWorkerProvider = factory.captureAvailableWorkers(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes(),
                sessionVariable.getComputationFragmentSchedulingPolicy(), workerProvider.getComputeResource());
        return candidateWorkerProvider;
    }

    private boolean isCacheSharingExpired(long cacheSharingWorkPeriod) {
        HistoricalNodeMgr historicalNodeMgr = GlobalStateMgr.getCurrentState().getHistoricalNodeMgr();
        ComputeResource computeResource = workerProvider.getComputeResource();

        long lastUpdateTime = historicalNodeMgr.getLastUpdateTime(computeResource.getWarehouseId(),
                computeResource.getWorkerGroupId());
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUpdateTime > cacheSharingWorkPeriod * 1000) {
            return true;
        }
        return false;
    }

    // re-balance scan ranges for compute node if needed, return the compute node which scan range is assigned to
    private ComputeNode reBalanceScanRangeForComputeNode(List<ComputeNode> backends, long avgNodeScanRangeBytes,
                                                         TScanRangeLocations scanRangeLocations) {
        if (backends == null || backends.isEmpty()) {
            return null;
        }

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        boolean forceReBalance = sessionVariable.getHdfsBackendSelectorForceRebalance();
        boolean enableDataCache = sessionVariable.isEnableScanDataCache();
        // If force re-balancing is not specified and cache is used, skip the rebalancing directly.
        if (!forceReBalance && enableDataCache) {
            return backends.get(0);
        }

        ComputeNode node = null;
        // For StarRocksScanNode, we currently assume size=1 for load balancing
        long addedScans = 1;
        
        for (ComputeNode backend : backends) {
            long assignedScanRanges = assignedBytesPerComputeNode.get(backend);
            if (assignedScanRanges + addedScans < avgNodeScanRangeBytes * kMaxImbalanceRatio) {
                node = backend;
                break;
            }
        }
        if (node == null) {
            node = backends.get(0);
        }
        return node;
    }

    static class ComputeNodeFunnel implements Funnel<ComputeNode> {
        @Override
        public void funnel(ComputeNode computeNode, PrimitiveSink primitiveSink) {
            primitiveSink.putString(computeNode.getHost(), StandardCharsets.UTF_8);
            primitiveSink.putInt(computeNode.getBePort());
        }
    }

    class TScanRangeLocationsFunnel implements Funnel<TScanRangeLocations> {
        @Override
        public void funnel(TScanRangeLocations tScanRangeLocations, PrimitiveSink primitiveSink) {
            starRocksScanRangeHasher.acceptScanRangeLocations(tScanRangeLocations, primitiveSink);
        }
    }

    public HashRing makeHashRing(Collection<ComputeNode> nodes) {
        HashRing hashRing = null;
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        String hashAlgorithm = sessionVariable.getHdfsBackendSelectorHashAlgorithm();
        int virtualNodeNum = sessionVariable.getConsistentHashVirtualNodeNum();
        if (hashAlgorithm.equalsIgnoreCase("rendezvous")) {
            hashRing = new RendezvousHashRing(Hashing.murmur3_128(), new TScanRangeLocationsFunnel(),
                    new ComputeNodeFunnel(), nodes);
        } else {
            hashRing = new ConsistentHashRing(Hashing.murmur3_128(), new TScanRangeLocationsFunnel(),
                    new ComputeNodeFunnel(), nodes, virtualNodeNum);
        }
        return hashRing;
    }

    private long computeTotalSize() {
        // For StarRocksScanNode, we use count as size for now
        return locations.size();
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {
        try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "computeScanRangeAssignment")) {
            computeGeneralAssignment();
            if (useIncrementalScanRanges) {
                boolean hasMore = scanNode.hasMoreScanRanges();
                TScanRangeParams end = new TScanRangeParams();
                end.setScan_range(new TScanRange());
                end.setEmpty(true);
                end.setHas_more(hasMore);
                for (ComputeNode computeNode : workerProvider.getAllWorkers()) {
                    assignment.put(computeNode.getId(), scanNode.getId().asInt(), end);
                }
            }
        }
    }

    private void computeGeneralAssignment() throws StarRocksException {
        if (locations.size() == 0) {
            return;
        }

        long totalSize = computeTotalSize();
        long avgNodeScanRangeBytes = totalSize / Math.max(workerProvider.getAllWorkers().size(), 1) + 1;
        for (ComputeNode computeNode : workerProvider.getAllWorkers()) {
            assignedBytesPerComputeNode.put(computeNode, 0L);
            assignedScanRangesPerComputeNode.put(computeNode, 0L);
            reBalancedBytesPerComputeNode.put(computeNode, 0L);
        }

        List<TScanRangeLocations> remoteScanRangeLocations = locations;
        
        // Use consistent hashing to schedule remote scan ranges
        HashRing hashRing = makeHashRing(assignedBytesPerComputeNode.keySet());
        HashRing candidateHashRing = null;
        if (candidateWorkerProvider != null) {
            Collection<ComputeNode> candidateWorkers = candidateWorkerProvider.getAllWorkers();
            if (!candidateWorkers.isEmpty()) {
                candidateHashRing = makeHashRing(candidateWorkers);
            }
        }

        if (shuffleScanRange) {
            Collections.shuffle(remoteScanRangeLocations);
        }
        // assign scan ranges.
        for (int i = 0; i < remoteScanRangeLocations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = remoteScanRangeLocations.get(i);
            List<ComputeNode> backends = hashRing.get(scanRangeLocations, kCandidateNumber);
            ComputeNode node = reBalanceScanRangeForComputeNode(backends, avgNodeScanRangeBytes, scanRangeLocations);
            if (node == null) {
                throw new StarRocksException("Failed to find backend to execute");
            }

            ComputeNode candidateNode = null;
            if (candidateHashRing != null) {
                List<ComputeNode> candidateBackends = candidateHashRing.get(scanRangeLocations, kCandidateNumber);
                // if data cache is enabled, skip re-balancing because it makes the cache position undefined.
                candidateNode = candidateBackends.get(0);
            }
            recordScanRangeAssignment(node, candidateNode, backends, scanRangeLocations);
        }

        recordScanRangeStatistic();
    }

    private void recordScanRangeAssignment(ComputeNode worker, ComputeNode candidateWorker, List<ComputeNode> backends,
                                           TScanRangeLocations scanRangeLocations)
            throws NonRecoverableException {
        workerProvider.selectWorker(worker.getId());

        // update statistic
        long addedScans = 1; // Always 1 for StarRocksScanNode for now
        
        assignedBytesPerComputeNode.put(worker, assignedBytesPerComputeNode.get(worker) + addedScans);
        // the fist item in backends will be assigned if there is no re-balance, we compute re-balance bytes
        // if the worker is not the first item in backends.
        if (worker != backends.get(0)) {
            reBalancedBytesPerComputeNode.put(worker, reBalancedBytesPerComputeNode.get(worker) + addedScans);
        }

        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        if (candidateWorker != null && !scanRangeLocations.scan_range.isSetInternal_scan_range()) {
            scanRangeParams.scan_range.hdfs_scan_range.setCandidate_node(
                    String.format("%s:%d", candidateWorker.getHost(), candidateWorker.getBrpcPort()));
        }
        assignment.put(worker.getId(), scanNode.getId().asInt(), scanRangeParams);
        assignedScanRangesPerComputeNode.put(worker,
                assignedScanRangesPerComputeNode.get(worker) + 1);
    }

    private void recordScanRangeStatistic() {
        // record scan range size for each backend
        for (Map.Entry<ComputeNode, Long> entry : assignedBytesPerComputeNode.entrySet()) {
            String host = entry.getKey().getAddress().hostname.replace('.', '_');
            long value = entry.getValue();
            String key = String.format("Placement.%s.assign.%s", scanNode.getTableName(), host);
            Tracers.count(Tracers.Module.EXTERNAL, key, value);
        }
        // record re-balance bytes for each backend
        for (Map.Entry<ComputeNode, Long> entry : reBalancedBytesPerComputeNode.entrySet()) {
            String host = entry.getKey().getAddress().hostname.replace('.', '_');
            long value = entry.getValue();
            String key = String.format("Placement.%s.balance.%s", scanNode.getTableName(), host);
            Tracers.count(Tracers.Module.EXTERNAL, key, value);
        }
        // record split number for each backend
        for (Map.Entry<ComputeNode, Long> entry : assignedScanRangesPerComputeNode.entrySet()) {
            String host = entry.getKey().getAddress().hostname.replace('.', '_');
            long value = entry.getValue();
            String key = String.format("Placement.%s.split.%s", scanNode.getTableName(), host);
            Tracers.count(Tracers.Module.EXTERNAL, key, value);
        }
    }
}

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

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.thrift.TConnectorScanNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Placeholder scan node for the RPC-based StarRocks external catalog.
 * <p>
 * The execution wiring will be filled in a later iteration; for now we surface the plan metadata so that
 * the planner/explain pipeline can reason about StarRocks tables without special casing.
 */
public class StarRocksScanNode extends ScanNode {
    private final StarRocksExternalTable table;
    private final List<TScanRangeLocations> scanRanges = new ArrayList<>();
    private String opaquedQueryPlan;
    private Map<String, String> executionProperties = Collections.emptyMap();
    private int tupleId = -1;

    public StarRocksScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                             StarRocksExternalTable table) {
        super(id, desc, planNodeName);
        this.table = table;
    }

    public StarRocksExternalTable getTable() {
        return table;
    }

    public void setScanRanges(List<TScanRangeLocations> ranges) {
        scanRanges.clear();
        if (ranges != null) {
            scanRanges.addAll(ranges);
        }
    }

    public void setOpaquedQueryPlan(String opaquedQueryPlan) {
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public void setExecutionProperties(Map<String, String> executionProperties) {
        this.executionProperties = executionProperties == null ? Collections.emptyMap() : executionProperties;
    }

    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this)
                .addValue(super.debugString())
                .add("table", table.getName())
                .add("catalog", table.getCatalogName())
                .add("tablets", table.getTablets().size())
                .toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return Collections.unmodifiableList(scanRanges);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.CONNECTOR_SCAN_NODE;
        TConnectorScanNode connectorNode = new TConnectorScanNode();
        connectorNode.setConnector_name(table.getCatalogName() == null ? "" : table.getCatalogName());
        if (tupleId >= 0) {
            connectorNode.setTuple_id(tupleId);
        }
        connectorNode.setConnector_name("starrocks");
        connectorNode.setDb_name(table.getCatalogDBName());
        connectorNode.setTable_name(table.getCatalogTableName());
        if (opaquedQueryPlan != null) {
            connectorNode.setOpaqued_query_plan(opaquedQueryPlan);
        }
        if (executionProperties != null && !executionProperties.isEmpty()) {
            connectorNode.setProperties(executionProperties);
        }
        msg.connector_scan_node = connectorNode;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append("TABLE: ").append(table.getName()).append("\n");
        if (table.getCatalogName() != null) {
            builder.append(prefix).append("CATALOG: ").append(table.getCatalogName()).append("\n");
        }
        builder.append(prefix).append("OPAQUED PLAN SIZE: ")
                .append(opaquedQueryPlan == null ? 0 : opaquedQueryPlan.length())
                .append("\n");
        builder.append(prefix).append("TABLET COUNT: ").append(table.getTablets().size()).append("\n");
        builder.append(prefix).append(String.format("cardinality=%s", cardinality)).append("\n");
        builder.append(prefix).append(String.format("avgRowSize=%s", avgRowSize)).append("\n");
        return builder.toString();
    }
}

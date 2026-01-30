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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.connector.starrocks.StarRocksExternalTable;
import com.starrocks.http.rest.v2.vo.ColumnView;
import com.starrocks.http.rest.v2.vo.DistributionInfoView;
import com.starrocks.http.rest.v2.vo.PartitionInfoView;
import com.starrocks.http.rest.v2.vo.TableSchemaView;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TStarRocksPartition;
import com.starrocks.thrift.TStarRocksPartitionType;
import com.starrocks.thrift.TStarRocksTableSink;
import com.starrocks.thrift.TStarRocksTablet;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class StarRocksTableSink extends DataSink {
    private final StarRocksExternalTable table;
    private final TupleDescriptor tupleDesc;
    private final TableSchemaView schemaView;
    private final List<PartitionInfoView.PartitionView> partitions;
    private final long txnId;
    private final String label;
    private final Map<String, String> properties;

    private final List<String> partitionColumnNames;
    private final List<String> distributionColumnNames;
    private final TStarRocksPartitionType partitionType;
    private final Map<String, Type> columnTypeMap;

    public StarRocksTableSink(StarRocksExternalTable table,
                              TupleDescriptor tupleDesc,
                              TableSchemaView schemaView,
                              List<PartitionInfoView.PartitionView> partitions,
                              long txnId,
                              String label) {
        this.table = Objects.requireNonNull(table, "table is null");
        this.tupleDesc = Objects.requireNonNull(tupleDesc, "tupleDesc is null");
        this.schemaView = Objects.requireNonNull(schemaView, "schemaView is null");
        this.partitions = partitions == null ? List.of() : partitions;
        this.txnId = txnId;
        this.label = label;
        this.properties = table.getExecutionProperties() == null
                ? Collections.emptyMap()
                : table.getExecutionProperties();

        PartitionInfoView partitionInfo = schemaView.getPartitionInfo();
        this.partitionType = resolvePartitionType(partitionInfo);
        this.partitionColumnNames = resolvePartitionColumns(partitionInfo);
        this.distributionColumnNames = resolveDistributionColumns(schemaView.getDefaultDistributionInfo());
        this.columnTypeMap = buildColumnTypeMap(table.getFullSchema());
    }

    private static Map<String, Type> buildColumnTypeMap(List<Column> columns) {
        Map<String, Type> map = new HashMap<>();
        if (columns == null) {
            return map;
        }
        for (Column column : columns) {
            if (column == null) {
                continue;
            }
            map.put(column.getName().toLowerCase(Locale.ROOT), column.getType());
        }
        return map;
    }

    private static TStarRocksPartitionType resolvePartitionType(PartitionInfoView partitionInfo) {
        if (partitionInfo == null || partitionInfo.getType() == null) {
            return TStarRocksPartitionType.UNPARTITIONED;
        }
        String type = partitionInfo.getType().toUpperCase(Locale.ROOT);
        return switch (type) {
            case "RANGE" -> TStarRocksPartitionType.RANGE;
            case "LIST" -> TStarRocksPartitionType.LIST;
            default -> TStarRocksPartitionType.UNPARTITIONED;
        };
    }

    private static List<String> resolvePartitionColumns(PartitionInfoView partitionInfo) {
        if (partitionInfo == null || partitionInfo.getPartitionColumns() == null) {
            return List.of();
        }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (ColumnView column : partitionInfo.getPartitionColumns()) {
            if (column != null && column.getName() != null) {
                builder.add(column.getName());
            }
        }
        return builder.build();
    }

    private static List<String> resolveDistributionColumns(DistributionInfoView distributionInfo) {
        if (distributionInfo == null || distributionInfo.getDistributionColumns() == null) {
            return List.of();
        }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (ColumnView column : distributionInfo.getDistributionColumns()) {
            if (column != null && column.getName() != null) {
                builder.add(column.getName());
            }
        }
        return builder.build();
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append("STARROCKS TABLE SINK\n");
        builder.append(prefix).append("  TABLE: ")
                .append(table.getCatalogDBName()).append('.').append(table.getCatalogTableName()).append("\n");
        builder.append(prefix).append("  TUPLE ID: ").append(tupleDesc.getId()).append("\n");
        builder.append(prefix).append("  ").append(DataPartition.RANDOM.getExplainString(explainLevel));
        return builder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink dataSink = new TDataSink(TDataSinkType.STARROCKS_TABLE_SINK);
        TStarRocksTableSink sink = new TStarRocksTableSink();
        sink.setTxn_id(txnId);
        if (label != null) {
            sink.setLabel(label);
        }
        sink.setTuple_id(tupleDesc.getId().asInt());
        sink.setPartition_type(partitionType);
        if (!partitionColumnNames.isEmpty()) {
            sink.setPartition_column_names(partitionColumnNames);
        }
        if (!distributionColumnNames.isEmpty()) {
            sink.setDistribution_column_names(distributionColumnNames);
        }
        sink.setDb_name(table.getCatalogDBName());
        sink.setTable_name(table.getCatalogTableName());
        if (properties != null && !properties.isEmpty()) {
            sink.setProperties(properties);
        }

        sink.setPartitions(buildPartitions());
        dataSink.setStarrocks_table_sink(sink);
        return dataSink;
    }

    private List<TStarRocksPartition> buildPartitions() {
        if (partitions == null || partitions.isEmpty()) {
            return List.of();
        }
        List<TStarRocksPartition> result = new ArrayList<>(partitions.size());
        for (PartitionInfoView.PartitionView partitionView : partitions) {
            if (partitionView == null) {
                continue;
            }
            TStarRocksPartition partition = new TStarRocksPartition();
            if (partitionView.getId() != null) {
                partition.setId(partitionView.getId());
            }
            if (partitionView.getName() != null) {
                partition.setName(partitionView.getName());
            }
            if (partitionView.getBucketNum() != null) {
                partition.setBucket_num(partitionView.getBucketNum());
            }
            if (partitionView.getDistributionType() != null) {
                partition.setDistribution_type(partitionView.getDistributionType());
            }
            if (partitionView.getMinPartition() != null) {
                partition.setIs_min_partition(partitionView.getMinPartition());
            }
            if (partitionView.getMaxPartition() != null) {
                partition.setIs_max_partition(partitionView.getMaxPartition());
            }
            if (partitionView.getStoragePath() != null) {
                partition.setStorage_path(partitionView.getStoragePath());
            }
            if (partitionType == TStarRocksPartitionType.RANGE) {
                List<String> startKeys = normalizePartitionValues(partitionView.getStartKeys());
                List<String> endKeys = normalizePartitionValues(partitionView.getEndKeys());
                if (!startKeys.isEmpty()) {
                    partition.setStart_keys(startKeys);
                }
                if (!endKeys.isEmpty()) {
                    partition.setEnd_keys(endKeys);
                }
            } else if (partitionType == TStarRocksPartitionType.LIST) {
                List<List<String>> inKeys = normalizePartitionValueSets(partitionView.getInKeys());
                if (!inKeys.isEmpty()) {
                    partition.setIn_keys(inKeys);
                }
            }
            partition.setTablets(buildTablets(partitionView));
            result.add(partition);
        }
        return result;
    }

    private List<TStarRocksTablet> buildTablets(PartitionInfoView.PartitionView partitionView) {
        if (partitionView.getTablets() == null || partitionView.getTablets().isEmpty()) {
            return List.of();
        }
        List<TStarRocksTablet> tablets = new ArrayList<>(partitionView.getTablets().size());
        for (var tabletView : partitionView.getTablets()) {
            if (tabletView == null || tabletView.getId() == null) {
                continue;
            }
            Long backendId = resolveBackendId(tabletView);
            Preconditions.checkState(backendId != null && backendId > 0,
                    "Missing backend id for tablet " + tabletView.getId());
            TStarRocksTablet tablet = new TStarRocksTablet();
            tablet.setTablet_id(tabletView.getId());
            tablet.setBackend_id(backendId);
            tablets.add(tablet);
        }
        return tablets;
    }

    private Long resolveBackendId(com.starrocks.http.rest.v2.vo.TabletView tabletView) {
        if (tabletView.getPrimaryComputeNodeId() != null) {
            return tabletView.getPrimaryComputeNodeId();
        }
        if (tabletView.getBackendIds() != null && !tabletView.getBackendIds().isEmpty()) {
            return tabletView.getBackendIds().iterator().next();
        }
        return null;
    }

    private List<String> normalizePartitionValues(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        if (partitionColumnNames.isEmpty()) {
            return values.stream().map(this::stringifyPartitionValue).toList();
        }
        if (values.size() != partitionColumnNames.size()) {
            return values.stream().map(this::stringifyPartitionValue).toList();
        }
        List<String> normalized = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            String columnName = partitionColumnNames.get(i);
            Type columnType = columnTypeMap.getOrDefault(columnName.toLowerCase(Locale.ROOT), null);
            normalized.add(stringifyPartitionValue(values.get(i), columnType));
        }
        return normalized;
    }

    private List<List<String>> normalizePartitionValueSets(List<List<Object>> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        List<List<String>> normalized = new ArrayList<>(values.size());
        for (List<Object> entry : values) {
            normalized.add(normalizePartitionValues(entry));
        }
        return normalized;
    }

    private String stringifyPartitionValue(Object value) {
        return stringifyPartitionValue(value, null);
    }

    private String stringifyPartitionValue(Object value, Type columnType) {
        if (value == null) {
            return null;
        }
        if (columnType != null && columnType.isScalarType()) {
            PrimitiveType primitive = columnType.getPrimitiveType();
            if (primitive == PrimitiveType.DATE) {
                return convertDateNumber(value);
            }
            if (primitive == PrimitiveType.DATETIME) {
                return convertDateTimeNumber(value);
            }
        }
        return Objects.toString(value, null);
    }

    private String convertDateNumber(Object value) {
        if (!(value instanceof Number)) {
            return Objects.toString(value, null);
        }
        long encoded = ((Number) value).longValue();
        long year = encoded / (16L * 32L);
        long month = (encoded / 32L) % 16L;
        long day = encoded % 32L;
        return String.format(Locale.ROOT, "%04d-%02d-%02d", year, month, day);
    }

    private String convertDateTimeNumber(Object value) {
        if (!(value instanceof Number)) {
            return Objects.toString(value, null);
        }
        long encoded = ((Number) value).longValue();
        String digits = String.format(Locale.ROOT, "%014d", encoded);
        return String.format(Locale.ROOT, "%s-%s-%s %s:%s:%s",
                digits.substring(0, 4),
                digits.substring(4, 6),
                digits.substring(6, 8),
                digits.substring(8, 10),
                digits.substring(10, 12),
                digits.substring(12, 14));
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}

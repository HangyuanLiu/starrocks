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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.ExternalPartitionKeyResolver;
import com.starrocks.connector.ExternalPartitionMappingContext;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionKeyResolutionPath;
import com.starrocks.connector.PartitionKeyResolutionResult;
import com.starrocks.connector.PartitionUtil;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves Iceberg base partitions to MV partition keys.
 * <p>
 * Phase 1: only handles current-spec direct mapping (same logic as default path,
 * but routed here so future phases have a natural attachment point).
 * <p>
 * Future phases will add resolution methods in this order:
 * <ol>
 *   <li>{@code resolveByCurrentSpec} (current phase)</li>
 *   <li>{@code resolveByHistoricalSpec}</li>
 *   <li>{@code resolveByPartitionExprFallback}</li>
 *   <li>{@code resolveBySyntheticTransform}</li>
 * </ol>
 */
public class IcebergPartitionKeyResolver implements ExternalPartitionKeyResolver {

    public static final IcebergPartitionKeyResolver INSTANCE = new IcebergPartitionKeyResolver();

    private IcebergPartitionKeyResolver() {
    }

    @Override
    public PartitionKeyResolutionResult resolve(ExternalPartitionMappingContext mappingContext,
                                                String basePartitionName)
            throws AnalysisException {
        PartitionInfo partitionInfo = mappingContext.getPartitionInfo(basePartitionName).orElse(null);
        if (partitionInfo != null) {
            return resolveByHistoricalOrFallbackSpec(mappingContext, basePartitionName, partitionInfo);
        }
        return resolveByCurrentSpec(mappingContext, basePartitionName);
    }

    private PartitionKeyResolutionResult resolveByCurrentSpec(ExternalPartitionMappingContext mappingContext,
                                                              String basePartitionName)
            throws AnalysisException {
        List<String> basePartitionValues = PartitionUtil.toPartitionValues(basePartitionName);
        List<Integer> mvRefBasePartitionColumnIndexes = mappingContext.getMvRefBasePartitionColumnIndexes();

        PartitionKey mvPartitionKey = PartitionUtil.createPartitionKey(
                mvRefBasePartitionColumnIndexes.stream().map(basePartitionValues::get).collect(Collectors.toList()),
                mvRefBasePartitionColumnIndexes.stream()
                        .map(mappingContext.getBaseTablePartitionColumns()::get)
                        .collect(Collectors.toList()),
                mappingContext.getBaseTable());

        return PartitionKeyResolutionResult.of(mvPartitionKey, PartitionKeyResolutionPath.ICEBERG_CURRENT_SPEC);
    }

    private PartitionKeyResolutionResult resolveByHistoricalOrFallbackSpec(ExternalPartitionMappingContext mappingContext,
                                                                           String basePartitionName,
                                                                           PartitionInfo partitionInfo)
            throws AnalysisException {
        IcebergTable icebergTable = (IcebergTable) mappingContext.getBaseTable();
        List<String> basePartitionValues = PartitionUtil.toPartitionValues(basePartitionName);
        List<Integer> mvRefBasePartitionColumnIndexes = mappingContext.getMvRefBasePartitionColumnIndexes();
        List<Column> mvPartitionColumns = mvRefBasePartitionColumnIndexes.stream()
                .map(mappingContext.getBaseTablePartitionColumns()::get)
                .collect(Collectors.toList());
        List<String> mvPartitionValues = mvRefBasePartitionColumnIndexes.stream()
                .map(basePartitionValues::get)
                .collect(Collectors.toList());

        if (shouldForceSyntheticFallback(mappingContext, partitionInfo)) {
            List<PartitionKey> fallbackKeys = IcebergPartitionUtils.getFallbackPartitionKeys(
                    icebergTable, mvPartitionColumns, basePartitionName, getSpecId(partitionInfo));
            if (!fallbackKeys.isEmpty()) {
                return PartitionKeyResolutionResult.of(fallbackKeys, PartitionKeyResolutionPath.ICEBERG_SYNTHETIC_TRANSFORM);
            }
            return PartitionKeyResolutionResult.of(fallbackKeys, PartitionKeyResolutionPath.ICEBERG_SYNTHETIC_TRANSFORM);
        }

        try {
            PartitionKey mvPartitionKey = IcebergPartitionUtils.createPartitionKey(
                    icebergTable, mvPartitionColumns, mvPartitionValues, partitionInfo);
            return PartitionKeyResolutionResult.of(mvPartitionKey, PartitionKeyResolutionPath.ICEBERG_HISTORICAL_SPEC);
        } catch (Exception e) {
            List<PartitionKey> fallbackKeys = IcebergPartitionUtils.getFallbackPartitionKeys(
                    icebergTable, mvPartitionColumns, basePartitionName, getSpecId(partitionInfo));
            if (!fallbackKeys.isEmpty()) {
                return PartitionKeyResolutionResult.of(fallbackKeys, PartitionKeyResolutionPath.ICEBERG_SYNTHETIC_TRANSFORM);
            }
            throw e;
        }
    }

    private boolean shouldForceSyntheticFallback(ExternalPartitionMappingContext mappingContext,
                                                 PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof com.starrocks.connector.iceberg.Partition icebergPartition)) {
            return false;
        }
        if (mappingContext.getMvRefBasePartitionColumns().size() != 1) {
            return false;
        }
        IcebergTable icebergTable = (IcebergTable) mappingContext.getBaseTable();
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        int specId = icebergPartition.getSpecId();
        if (specId < 0 || nativeTable.spec().specId() == specId) {
            return false;
        }

        Column refPartitionColumn = mappingContext.getMvRefBasePartitionColumns().get(0);
        org.apache.iceberg.types.Types.NestedField sourceField =
                nativeTable.schema().findField(refPartitionColumn.getName());
        if (sourceField == null) {
            return false;
        }

        org.apache.iceberg.PartitionField currentField = nativeTable.spec().fields().stream()
                .filter(field -> field.sourceId() == sourceField.fieldId() && !field.transform().isVoid())
                .findFirst()
                .orElse(null);
        if (currentField == null) {
            return false;
        }

        org.apache.iceberg.PartitionSpec actualSpec = nativeTable.specs().get(specId);
        if (actualSpec == null) {
            return false;
        }
        org.apache.iceberg.PartitionField actualField = actualSpec.fields().stream()
                .filter(field -> field.sourceId() == sourceField.fieldId() && !field.transform().isVoid())
                .findFirst()
                .orElse(null);

        String currentTransform = currentField.transform().toString();
        String actualTransform = actualField == null ? null : actualField.transform().toString();
        if (currentTransform.equals(actualTransform)) {
            return false;
        }

        IcebergPartitionTransform currentPartitionTransform = IcebergPartitionTransform.fromString(currentTransform);
        return currentPartitionTransform == IcebergPartitionTransform.BUCKET
                || currentPartitionTransform == IcebergPartitionTransform.TRUNCATE;
    }

    private Integer getSpecId(PartitionInfo partitionInfo) {
        if (partitionInfo instanceof com.starrocks.connector.iceberg.Partition icebergPartition) {
            return icebergPartition.getSpecId();
        }
        return null;
    }
}

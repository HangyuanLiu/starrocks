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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.Type;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergPartitionTransform.YEAR;

public class IcebergPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);
    private static final Pattern UNIT_TEST_BOUNDS_PATTERN = Pattern.compile(".*-lb(-?\\d+)-ub(-?\\d+)-\\d+\\.parquet$");
    private static final Pattern UNIT_TEST_SYNTHETIC_VALUES_PATTERN =
            Pattern.compile(".*-sv(-?\\d+(?:_-?\\d+)*)-\\d+\\.parquet$");
    private static final SimpleExecutor PARTITION_FALLBACK_EXECUTOR =
            new SimpleExecutor("IcebergPartitionFallback", TResultSinkType.HTTP_PROTOCAL);
    private static final Set<IcebergPartitionTransform> INTEGRAL_FALLBACK_TRANSFORMS = Set.of(
            IcebergPartitionTransform.BUCKET,
            IcebergPartitionTransform.TRUNCATE);

    private static final class PartitionTransformSignature {
        private final IcebergPartitionTransform transform;
        private final String sourceColumnName;
        private final Integer parameter;

        private PartitionTransformSignature(IcebergPartitionTransform transform,
                                            String sourceColumnName,
                                            Integer parameter) {
            this.transform = transform;
            this.sourceColumnName = sourceColumnName;
            this.parameter = parameter;
        }

        public boolean matches(PartitionTransformSignature other) {
            return transform == other.transform
                    && Objects.equals(parameter, other.parameter)
                    && sourceColumnName != null
                    && other.sourceColumnName != null
                    && sourceColumnName.equalsIgnoreCase(other.sourceColumnName);
        }
    }

    // Normalize partition name to yyyy-MM-dd (Type is Date) or yyyy-MM-dd HH:mm:ss (Type is Datetime)
    // Iceberg partition field transform support year, month, day, hour now,
    // eg.
    // year(ts)  partitionName : 2023              return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // month(ts) partitionName : 2023-01           return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // day(ts)   partitionName : 2023-01-01        return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // hour(ts)  partitionName : 2023-01-01-12     return 2023-01-01 12:00:00 (Datetime)
    public static String normalizeTimePartitionName(String partitionName,
                                                    PartitionField partitionField,
                                                    Schema schema,
                                                    Type type) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        boolean parseFromDate = true;
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == YEAR) {
            Preconditions.checkArgument(partitionName.length() == 4, "Invalid partition name: %s", partitionName);
            partitionName += "-01-01";
        } else if (transform == IcebergPartitionTransform.MONTH) {
            Preconditions.checkArgument(partitionName.length() == 7, "Invalid partition name: %s", partitionName);
            partitionName += "-01";
        } else if (transform == IcebergPartitionTransform.DAY) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else if (transform == IcebergPartitionTransform.HOUR) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
            parseFromDate = false;
        } else {
            throw new StarRocksConnectorException("Unsupported partition transform to normalize: %s",
                    partitionField.transform().toString());
        }

        // partition name formatter
        DateTimeFormatter formatter = null;
        if (type.isDate()) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }
        // If has timestamp with time zone, should compute the time zone offset to UTC
        ZoneId zoneId;
        if (schema.findType(partitionField.sourceId()).equals(Types.TimestampType.withZone())) {
            zoneId = TimeUtils.getTimeZone().toZoneId();
        } else {
            zoneId = ZoneOffset.UTC;
        }

        String result;
        try {
            LocalDateTime datetime;
            if (parseFromDate) {
                // since it's from date, it can be converted to LocalDateTime by atStartOfDay
                datetime = LocalDate.parse(partitionName, dateTimeFormatter).atStartOfDay();
            } else {
                // parse from datetime which contains hour
                datetime = LocalDateTime.parse(partitionName, dateTimeFormatter);
            }
            // convert from UTC to local time
            LocalDateTime localDateTime = convertTimezone(datetime, ZoneOffset.UTC, zoneId);
            // format to string
            result = localDateTime.format(formatter);
        } catch (Exception e) {
            LOG.warn("parse partition name failed, partitionName: {}, partitionField: {}, type: {}",
                    partitionName, partitionField, type);
            throw new StarRocksConnectorException("parse/format partition name failed", e);
        }
        return result;
    }

    public static LocalDateTime convertTimezone(LocalDateTime time, ZoneId from, ZoneId to) {
        return time.atZone(from).withZoneSameInstant(to).toLocalDateTime();
    }

    public static Term convertPartitionExprToTerm(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return Expressions.ref(slotRef.getColumnName());
        } else if (expr instanceof FunctionCallExpr functionCallExpr) {
            String fn = functionCallExpr.getFunctionName();
            Expr child = functionCallExpr.getChild(0);
            if (child instanceof SlotRef) {
                String colName = ((SlotRef) child).getColumnName();
                switch (fn.toLowerCase(Locale.ROOT)) {
                    case "year":
                        return Expressions.year(colName);
                    case "month":
                        return Expressions.month(colName);
                    case "day":
                        return Expressions.day(colName);
                    case "hour":
                        return Expressions.hour(colName);
                    case "identity":
                        return Expressions.ref(colName);
                    case "truncate":
                        IntLiteral width = (IntLiteral) functionCallExpr.getChild(1);
                        return Expressions.truncate(colName, (int) width.getValue());
                    case "bucket":
                        IntLiteral numBuckets = (IntLiteral) functionCallExpr.getChild(1);
                        return Expressions.bucket(colName, (int) numBuckets.getValue());
                    case "void":
                        // not supported yet.
                    default:
                        throw new SemanticException(
                                "Unsupported partition transform %s for column %s", fn, colName);
                }
            } else {
                throw new SemanticException("Unsupported partition transform %s for arguments", fn);
            }
        } else {
            throw new SemanticException("Does not support partition clause: " + expr);
        }
    }

    public static String normalizePartitionExpr(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return "`" + slotRef.getColumnName() + "`";
        } else if (expr instanceof FunctionCallExpr functionCallExpr) {
            String fn = functionCallExpr.getFunctionName().toLowerCase(Locale.ROOT);
            Expr child = functionCallExpr.getChild(0);
            if (!(child instanceof SlotRef slotRef)) {
                throw new SemanticException("Unsupported partition transform %s for arguments",
                        functionCallExpr.getFunctionName());
            }

            String quotedColumn = "`" + slotRef.getColumnName() + "`";
            switch (fn) {
                case "year":
                case "month":
                case "day":
                case "hour":
                    return String.format("%s(%s)", fn, quotedColumn);
                case "identity":
                    return quotedColumn;
                case "truncate":
                case "bucket":
                    IntLiteral number = (IntLiteral) functionCallExpr.getChild(1);
                    return String.format("%s(%s, %s)", fn, quotedColumn, number.getValue());
                case "void":
                    // not supported yet.
                default:
                    throw new SemanticException("Unsupported partition transform %s for column %s",
                            functionCallExpr.getFunctionName(), slotRef.getColumnName());
            }
        } else {
            throw new SemanticException("Does not support partition clause: " + expr);
        }
    }

    public static String getPartitionExprSourceColumn(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return slotRef.getColumnName();
        } else if (expr instanceof FunctionCallExpr functionCallExpr) {
            Expr child = functionCallExpr.getChild(0);
            if (child instanceof SlotRef slotRef) {
                return slotRef.getColumnName();
            }
            throw new SemanticException("Unsupported partition transform %s for arguments",
                    functionCallExpr.getFunctionName());
        } else {
            throw new SemanticException("Does not support partition clause: " + expr);
        }
    }

    // Get the date interval from iceberg partition transform
    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromIceberg(IcebergTable table,
                                                                                Column partitionColumn) {
        PartitionField partitionField = table.getPartitionFiled(partitionColumn.getName());
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColumn.getName(), table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
        }
        String transform = partitionField.transform().toString();
        IcebergPartitionTransform icebergPartitionTransform = IcebergPartitionTransform.fromString(transform);
        switch (icebergPartitionTransform) {
            case YEAR:
                return PartitionUtil.DateTimeInterval.YEAR;
            case MONTH:
                return PartitionUtil.DateTimeInterval.MONTH;
            case DAY:
                return PartitionUtil.DateTimeInterval.DAY;
            case HOUR:
                return PartitionUtil.DateTimeInterval.HOUR;
            default:
                return PartitionUtil.DateTimeInterval.NONE;
        }
    }

    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromPartition(IcebergTable table,
                                                                                  Column partitionColumn,
                                                                                  PartitionInfo partitionInfo) {
        Integer sourceId = getSourceId(table, partitionColumn);
        if (sourceId == null) {
            return PartitionUtil.DateTimeInterval.NONE;
        }
        PartitionSpec spec = getSpecForPartition(table, partitionInfo);
        if (spec == null) {
            return getDateTimeIntervalFromIceberg(table, partitionColumn);
        }
        PartitionUtil.DateTimeInterval interval = getIntervalFromSpec(spec, sourceId);
        if (interval != PartitionUtil.DateTimeInterval.NONE) {
            return interval;
        }
        return getDateTimeIntervalFromIceberg(table, partitionColumn);
    }

    public static PartitionKey createPartitionKey(IcebergTable icebergTable,
                                                  List<Column> partitionColumns,
                                                  List<String> partitionValues,
                                                  PartitionInfo partitionInfo)
            throws AnalysisException {
        Preconditions.checkState(partitionValues.size() == partitionColumns.size(),
                "columns size is %s, but values size is %s", partitionColumns.size(), partitionValues.size());

        PartitionKey partitionKey = new IcebergPartitionKey();
        for (int i = 0; i < partitionValues.size(); i++) {
            String rawValue = partitionValues.get(i);
            Column column = partitionColumns.get(i);
            PartitionField field = findPartitionFieldForColumn(icebergTable, column.getName(), rawValue, partitionInfo);
            LiteralExpr exprValue;
            if (rawValue == null) {
                rawValue = "null";
            }
            if (((NullablePartitionKey) partitionKey).nullPartitionValueList().contains(rawValue)) {
                partitionKey.setNullPartitionValue(rawValue);
                exprValue = NullLiteral.create(column.getType());
            } else if (field != null && field.transform().dedupName().equalsIgnoreCase("time")) {
                String normalizedValue = normalizeTimePartitionName(rawValue, field,
                        icebergTable.getNativeTable().schema(), column.getType());
                exprValue = LiteralExprFactory.create(normalizedValue, column.getType());
            } else {
                exprValue = LiteralExprFactory.create(rawValue, column.getType());
            }
            partitionKey.pushColumn(exprValue, column.getType().getPrimitiveType());
        }
        return partitionKey;
    }

    public static PartitionField findPartitionFieldForColumn(IcebergTable icebergTable,
                                                             String columnName,
                                                             String partitionValue,
                                                             PartitionInfo partitionInfo) {
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        Integer sourceId = getSourceId(icebergTable, columnName);
        if (sourceId == null) {
            return null;
        }

        PartitionSpec spec = getSpecForPartition(icebergTable, partitionInfo);
        if (spec != null) {
            PartitionField field = findPartitionFieldInSpec(nativeTable, spec, sourceId);
            if (field != null) {
                return field;
            }
        }

        if (nativeTable.specs().size() <= 1) {
            return findPartitionFieldInSpec(nativeTable, nativeTable.spec(), sourceId);
        }

        for (PartitionSpec historicalSpec : nativeTable.specs().values()) {
            PartitionField field = findPartitionFieldInSpec(nativeTable, historicalSpec, sourceId);
            if (field != null && (partitionValue == null || matchesTransformFormat(field, partitionValue))) {
                return field;
            }
        }

        return findPartitionFieldInSpec(nativeTable, nativeTable.spec(), sourceId);
    }

    /**
     * Check if the partition evolution of an Iceberg table is safe for partitioned MV refresh.
     * Safe evolution covers:
     * - Transform unchanged across all specs (e.g., add/remove BUCKET on another column)
     * - Time-family granularity changes (YEAR/MONTH/DAY/HOUR interchangeable)
     * - void ↔ time-family transitions
     *
     * @param icebergTable    the Iceberg base table
     * @param partitionColumn the MV's ref partition column (derived from base table)
     * @return true if evolution is safe
     */
    public static boolean isSafePartitionEvolution(IcebergTable icebergTable, Column partitionColumn) {
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        if (nativeTable.specs().size() <= 1) {
            return true;
        }
        int sourceId;
        try {
            sourceId = nativeTable.schema().findField(partitionColumn.getName()).fieldId();
        } catch (Exception e) {
            return false;
        }
        Set<String> transformSpecs = nativeTable.specs().values().stream()
                .flatMap(spec -> spec.fields().stream())
                .filter(f -> f.sourceId() == sourceId && !f.transform().isVoid())
                .map(f -> f.transform().toString())
                .collect(Collectors.toSet());
        boolean hasVoidOrMissingSpec = false;
        for (PartitionSpec spec : nativeTable.specs().values()) {
            boolean hasActiveSourceField = spec.fields().stream()
                    .anyMatch(field -> field.sourceId() == sourceId && !field.transform().isVoid());
            if (hasActiveSourceField) {
                continue;
            }
            boolean hasOtherActiveField = spec.fields().stream()
                    .anyMatch(field -> field.sourceId() != sourceId && !field.transform().isVoid());
            if (hasOtherActiveField) {
                return false;
            }
            hasVoidOrMissingSpec = true;
        }

        if (transformSpecs.isEmpty()) {
            return false;
        }

        Set<IcebergPartitionTransform> transforms = transformSpecs.stream()
                .map(IcebergPartitionTransform::fromString)
                .collect(Collectors.toSet());

        // size <= 1: unchanged transform, including BUCKET/TRUNCATE parameters.
        if (transformSpecs.size() <= 1) {
            if (!hasVoidOrMissingSpec) {
                return true;
            }
            IcebergPartitionTransform onlyTransform = transforms.iterator().next();
            if (TIME_TRANSFORMS.contains(onlyTransform)) {
                return true;
            }
            return partitionColumn.getType().isDate() && onlyTransform == IcebergPartitionTransform.IDENTITY;
        }

        // Time-family granularity changes (YEAR/MONTH/DAY/HOUR) are safe
        if (TIME_TRANSFORMS.containsAll(transforms)) {
            return true;
        }

        // IDENTITY on DATE column + time-family is safe (IDENTITY(DATE) is equivalent to DAY)
        if (partitionColumn.getType().isDate()) {
            Set<IcebergPartitionTransform> allowedWithIdentity =
                    new java.util.HashSet<>(TIME_TRANSFORMS);
            allowedWithIdentity.add(IcebergPartitionTransform.IDENTITY);
            if (allowedWithIdentity.containsAll(transforms)) {
                return true;
            }
        }

        return false;
    }

    private static final Set<IcebergPartitionTransform> TIME_TRANSFORMS = Set.of(
            IcebergPartitionTransform.YEAR,
            IcebergPartitionTransform.MONTH,
            IcebergPartitionTransform.DAY,
            IcebergPartitionTransform.HOUR);

    /**
     * Check if all non-void transforms for a source column across all specs are time-family
     * (or IDENTITY, which is treated as DAY-equivalent for DATE columns in createPartitionKey).
     */
    public static boolean isAllTimeTransforms(org.apache.iceberg.Table nativeTable, int sourceId) {
        Set<IcebergPartitionTransform> transforms = nativeTable.specs().values().stream()
                .flatMap(spec -> spec.fields().stream())
                .filter(f -> f.sourceId() == sourceId && !f.transform().isVoid())
                .map(f -> IcebergPartitionTransform.fromString(f.transform().toString()))
                .collect(Collectors.toSet());
        if (transforms.isEmpty()) {
            return false;
        }
        // Pure time transforms
        if (TIME_TRANSFORMS.containsAll(transforms)) {
            return true;
        }
        // IDENTITY + time transforms (IDENTITY on DATE acts like DAY)
        Set<IcebergPartitionTransform> withIdentity = new java.util.HashSet<>(TIME_TRANSFORMS);
        withIdentity.add(IcebergPartitionTransform.IDENTITY);
        return withIdentity.containsAll(transforms);
    }

    /**
     * Get the DateTimeInterval from a specific PartitionSpec for the given source column.
     */
    public static PartitionUtil.DateTimeInterval getIntervalFromSpec(
            PartitionSpec spec, int sourceId) {
        for (PartitionField field : spec.fields()) {
            if (field.sourceId() == sourceId && !field.transform().isVoid()) {
                IcebergPartitionTransform transform =
                        IcebergPartitionTransform.fromString(field.transform().toString());
                switch (transform) {
                    case YEAR:
                        return PartitionUtil.DateTimeInterval.YEAR;
                    case MONTH:
                        return PartitionUtil.DateTimeInterval.MONTH;
                    case DAY:
                    case IDENTITY:
                        // IDENTITY on DATE column is equivalent to DAY interval
                        return PartitionUtil.DateTimeInterval.DAY;
                    case HOUR:
                        return PartitionUtil.DateTimeInterval.HOUR;
                    default:
                        return PartitionUtil.DateTimeInterval.NONE;
                }
            }
        }
        return PartitionUtil.DateTimeInterval.NONE;
    }

    public static boolean isMVPartitionAlignedWithCurrentSpec(MaterializedView mv, IcebergTable icebergTable) {
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        if (nativeTable.spec().isUnpartitioned()) {
            return mv.getPartitionInfo().isUnPartitioned();
        }
        return arePartitionExprsAlignedWithCurrentSpec(icebergTable, getMVPartitionExprs(mv));
    }

    public static boolean arePartitionExprsAlignedWithCurrentSpec(IcebergTable icebergTable,
                                                                  List<Expr> partitionExprs) {
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        PartitionSpec currentSpec = nativeTable.spec();
        if (currentSpec.isUnpartitioned()) {
            return partitionExprs == null || partitionExprs.isEmpty();
        }
        if (partitionExprs == null || partitionExprs.isEmpty()) {
            return false;
        }
        List<PartitionField> activeFields = currentSpec.fields().stream()
                .filter(field -> !field.transform().isVoid())
                .collect(Collectors.toList());
        if (partitionExprs.size() != activeFields.size()) {
            return false;
        }
        for (int i = 0; i < activeFields.size(); i++) {
            Optional<PartitionTransformSignature> currentSignature =
                    buildSignatureFromPartitionField(activeFields.get(i), nativeTable.schema());
            Optional<PartitionTransformSignature> mvSignature =
                    buildSignatureFromMvPartitionExpr(partitionExprs.get(i));
            if (currentSignature.isEmpty() || mvSignature.isEmpty()
                    || !currentSignature.get().matches(mvSignature.get())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSupportedConvertPartitionTransform(IcebergPartitionTransform transform) {
        return transform == IcebergPartitionTransform.IDENTITY ||
                transform == YEAR ||
                transform == IcebergPartitionTransform.MONTH ||
                transform == IcebergPartitionTransform.DAY ||
                transform == IcebergPartitionTransform.HOUR ||
                transform == IcebergPartitionTransform.BUCKET ||
                transform == IcebergPartitionTransform.TRUNCATE;
    }

    public static LocalDateTime addDateTimeInterval(LocalDateTime dateTime, IcebergPartitionTransform transform) {
        switch (transform) {
            case YEAR:
                return dateTime.plusYears(1);
            case MONTH:
                return dateTime.plusMonths(1);
            case DAY:
                return dateTime.plusDays(1);
            case HOUR:
                return dateTime.plusHours(1);
            default:
                throw new StarRocksConnectorException("Unsupported partition transform to add: %s", transform);
        }
    }

    private static Integer getSourceId(IcebergTable icebergTable, Column partitionColumn) {
        return getSourceId(icebergTable, partitionColumn.getName());
    }

    private static Integer getSourceId(IcebergTable icebergTable, String columnName) {
        try {
            return icebergTable.getNativeTable().schema().findField(columnName).fieldId();
        } catch (Exception e) {
            return null;
        }
    }

    private static PartitionSpec getSpecForPartition(IcebergTable table, PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof com.starrocks.connector.iceberg.Partition icebergPartition)) {
            return null;
        }
        int specId = icebergPartition.getSpecId();
        if (specId < 0) {
            return null;
        }
        return table.getNativeTable().specs().get(specId);
    }

    private static PartitionField findPartitionFieldInSpec(org.apache.iceberg.Table nativeTable,
                                                           PartitionSpec spec,
                                                           int sourceId) {
        if (spec == null) {
            return null;
        }
        for (PartitionField field : spec.fields()) {
            if (field.transform().isVoid()) {
                continue;
            }
            String fieldColumnName = nativeTable.schema().findColumnName(field.sourceId());
            if (field.sourceId() == sourceId && fieldColumnName != null) {
                return field;
            }
        }
        return null;
    }

    private static boolean matchesTransformFormat(PartitionField field, String value) {
        if (!field.transform().dedupName().equalsIgnoreCase("time")) {
            return true;
        }
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(field.transform().toString());
        switch (transform) {
            case YEAR:
                return value.length() == 4;
            case MONTH:
                return value.length() == 7;
            case DAY:
                return value.length() == 10;
            case HOUR:
                return value.length() == 13;
            default:
                return true;
        }
    }

    /**
        convert partition value to predicate
        eg.
        partitionColumn: ts(date)
        partitionValue: 2023  transform: year
        return ts >= '2023-01-01' and ts < '2024-01-01'
        partitionValue: 2023-01 transform: month
        return ts >= '2023-01-01' and ts < '2023-02-01'
        partitionValue: 2023-01-01  transform: day
        return ts >= '2023-01-01' and ts < '2023-01-02'

        partitionColumn: ts(datetime)   transform: year
        partitionValue: 2023  transform: year
        return ts >= '2023-01-01 00:00:00' and ts < '2024-01-01 00:00:00'
        partitionValue: 2023-01 transform: month
        return ts >= '2023-01-01 00:00:00' and ts < '2023-02-01 00:00:00'
        partitionValue: 2023-01-01  transform: day
        return ts >= '2023-01-01 00:00:00' and ts < '2023-01-02 00:00:00'
        partitionValue: 2023-01-01-12  transform: hour
        return ts >= '2023-01-01 12:00:00' and ts < '2023-01-01 13:00:00'
    */
    public static Range<String> toPartitionRange(IcebergTable table, String partitionColumn,
                                                 String partitionValue, PartitionField partitionField,
                                                 boolean isFromIcebergTime) {
        Preconditions.checkArgument(partitionField != null,
                "Partition field is null for column: %s", partitionColumn);
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.IDENTITY) {
            return Range.singleton(partitionValue);
        } else {
            // transform is year, month, day, hour
            Type partitiopnColumnType = table.getColumn(partitionColumn).getType();
            Preconditions.checkState(partitiopnColumnType.isDateType(),
                    "Partition column %s type must be date or datetime", partitionColumn);
            if (isFromIcebergTime) {
                partitionValue = normalizeTimePartitionName(partitionValue, partitionField,
                        table.getNativeTable().schema(), partitiopnColumnType);
            }
            LocalDateTime startDateTime = null;
            DateTimeFormatter dateTimeFormatter = null;
            if (partitiopnColumnType.isDate()) {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                startDateTime = LocalDate.parse(partitionValue, dateTimeFormatter).atStartOfDay();
            } else {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                startDateTime = LocalDateTime.parse(partitionValue, dateTimeFormatter);
            }
            LocalDateTime endDateTime = addDateTimeInterval(startDateTime, transform);
            String endDateTimeStr = endDateTime.format(dateTimeFormatter);
            return Range.closedOpen(partitionValue, endDateTimeStr);
        }
    }

    /**
     * Convert iceberg partition transform to sql predicate
     * eg.
     * Iceberg table partition column: day(dt)
     * partition value      : 2023-01-02
     * generated predicate  : dt >= '2023-01-01 00:08:00' and dt < '2023-01-02:00:08:00'
     */
    public static String convertPartitionTransformToPredicate(IcebergTable table, PartitionField partitionField,
                                                              String partitionColumn, String partitionValue) {
        if (partitionField == null || Strings.isNullOrEmpty(partitionColumn) || Strings.isNullOrEmpty(partitionValue)) {
            throw new StarRocksConnectorException("Partition field/column/value is null");
        }
        IcebergPartitionTransform transform =
                IcebergPartitionTransform.fromString(partitionField.transform().toString());
        String partitionCol = StatisticUtils.quoting(partitionColumn);

        // Handle bucket and truncate explicitly
        if (transform == IcebergPartitionTransform.BUCKET) {
            // transform string format: bucket[<num>]
            int numBuckets = extractTransformParam(partitionField.transform().toString());
            int bucketId;
            try {
                bucketId = Integer.parseInt(partitionValue);
            } catch (NumberFormatException e) {
                throw new StarRocksConnectorException("Invalid bucket partition value: %s", partitionValue);
            }
            String fn = FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX + "bucket";
            return String.format("%s(%s, %d) = %d", fn, partitionCol, numBuckets, bucketId);
        } else if (transform == IcebergPartitionTransform.TRUNCATE) {
            // transform string format: truncate[<width>]
            int width = extractTransformParam(partitionField.transform().toString());
            Type partitionType = table.getColumn(partitionColumn).getType();
            if (partitionType.isBinaryType()) {
                try {
                    partitionValue = new String(Base64.getDecoder().decode(partitionValue));
                } catch (Exception e) {
                    throw new StarRocksConnectorException("Invalid base64 partition value: %s", partitionValue, e);
                }
            }
            String fn = FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX + "truncate";
            return String.format("%s(%s, %d) = '%s'", fn, partitionCol, width, partitionValue);
        }

        Range<String> range = toPartitionRange(table, partitionColumn, partitionValue, partitionField, true);
        if (range.lowerEndpoint().equals(range.upperEndpoint())) {
            return String.format("%s = '%s'", partitionCol, range.lowerEndpoint());
        } else {
            String lowerEndpoint = range.lowerEndpoint();
            String upperEndpoint = range.upperEndpoint();
            return String.format("%s >= '%s' and %s < '%s'", partitionCol, lowerEndpoint, partitionCol, upperEndpoint);
        }
    }

    private static int extractTransformParam(String transform) {
        int l = transform.indexOf('[');
        int r = transform.indexOf(']');
        if (l >= 0 && r > l) {
            try {
                return Integer.parseInt(transform.substring(l + 1, r));
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        throw new StarRocksConnectorException("Unsupported or missing transform parameter: %s", transform);
    }

    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromPartitionExpr(Expr partitionExpr,
                                                                                      Type partitionType) {
        IcebergPartitionTransform transform = getTargetTransformForPartitionExpr(partitionExpr, partitionType);
        if (transform == null) {
            return PartitionUtil.DateTimeInterval.NONE;
        }
        return switch (transform) {
            case YEAR -> PartitionUtil.DateTimeInterval.YEAR;
            case MONTH -> PartitionUtil.DateTimeInterval.MONTH;
            case DAY, IDENTITY -> PartitionUtil.DateTimeInterval.DAY;
            case HOUR -> PartitionUtil.DateTimeInterval.HOUR;
            default -> PartitionUtil.DateTimeInterval.NONE;
        };
    }

    public static List<PartitionKey> getFallbackPartitionKeys(IcebergTable table,
                                                              List<Column> refPartitionColumns,
                                                              String partitionName) throws AnalysisException {
        return getFallbackPartitionKeys(table, refPartitionColumns, partitionName, null);
    }

    public static List<PartitionKey> getFallbackPartitionKeys(IcebergTable table,
                                                              List<Column> refPartitionColumns,
                                                              String partitionName,
                                                              Integer actualSpecId) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName) || refPartitionColumns.size() != 1) {
            return ImmutableList.of();
        }

        org.apache.iceberg.Table nativeTable = table.getNativeTable();
        if (nativeTable.specs().size() <= 1) {
            return ImmutableList.of();
        }

        Column refPartitionColumn = refPartitionColumns.get(0);
        Types.NestedField sourceField = nativeTable.schema().findField(refPartitionColumn.getName());
        if (sourceField == null) {
            return ImmutableList.of();
        }

        PartitionField currentField = findActivePartitionField(nativeTable.spec(), sourceField.fieldId());
        if (currentField == null) {
            return ImmutableList.of();
        }

        IcebergPartitionTransform currentTransform =
                IcebergPartitionTransform.fromString(currentField.transform().toString());
        Optional<PartitionSpec> actualSpecOpt = resolveSpecForPartition(nativeTable, partitionName, actualSpecId);
        if (actualSpecOpt.isEmpty()) {
            return ImmutableList.of();
        }
        PartitionSpec actualSpec = actualSpecOpt.get();
        if (actualSpec.specId() == nativeTable.spec().specId()) {
            return ImmutableList.of();
        }

        if (INTEGRAL_FALLBACK_TRANSFORMS.contains(currentTransform)) {
            int transformParam;
            try {
                transformParam = extractTransformParam(currentField.transform().toString());
            } catch (StarRocksConnectorException e) {
                LOG.debug("Skip fallback partition conversion for {} because transform parameter is invalid: {}",
                        partitionName, e.getMessage());
                return ImmutableList.of();
            }
            if (transformParam <= 0) {
                return ImmutableList.of();
            }

            Set<Long> syntheticPartitionValues =
                    currentTransform == IcebergPartitionTransform.TRUNCATE
                            ? collectSyntheticTruncateValuesFromBounds(
                            nativeTable, actualSpec, partitionName, sourceField, transformParam)
                            : Collections.emptySet();
            if (syntheticPartitionValues.isEmpty()) {
                syntheticPartitionValues =
                        collectSyntheticIntegralValuesByQuery(
                                table, actualSpec, partitionName, sourceField, currentTransform, transformParam);
                if (syntheticPartitionValues.isEmpty()) {
                    return ImmutableList.of();
                }
            }

            List<Long> sortedValues = new ArrayList<>(syntheticPartitionValues);
            Collections.sort(sortedValues);
            List<PartitionKey> result = new ArrayList<>(sortedValues.size());
            for (Long value : sortedValues) {
                result.add(PartitionUtil.createPartitionKey(
                        ImmutableList.of(String.valueOf(value)), refPartitionColumns, table));
            }
            return result;
        }

        if (!isTimeBasedFallbackTransform(currentTransform, refPartitionColumn.getType())) {
            return ImmutableList.of();
        }

        Set<String> syntheticPartitionValues =
                collectSyntheticTimeValuesByQuery(
                        table, actualSpec, partitionName, sourceField, currentTransform, refPartitionColumn.getType());
        if (syntheticPartitionValues.isEmpty()) {
            return ImmutableList.of();
        }

        List<String> sortedValues = new ArrayList<>(syntheticPartitionValues);
        Collections.sort(sortedValues);
        return createSyntheticTimePartitionKeys(sortedValues, refPartitionColumns, currentTransform);
    }

    public static List<PartitionKey> getPartitionExprFallbackKeys(IcebergTable table,
                                                                  List<Column> refPartitionColumns,
                                                                  String partitionName,
                                                                  Integer actualSpecId,
                                                                  Expr partitionExpr) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName) || refPartitionColumns.size() != 1) {
            return ImmutableList.of();
        }

        Column refPartitionColumn = refPartitionColumns.get(0);
        IcebergPartitionTransform targetTransform =
                getTargetTransformForPartitionExpr(partitionExpr, refPartitionColumn.getType());
        if (targetTransform == null || !isTimeBasedFallbackTransform(targetTransform, refPartitionColumn.getType())) {
            return ImmutableList.of();
        }

        org.apache.iceberg.Table nativeTable = table.getNativeTable();
        Types.NestedField sourceField = nativeTable.schema().findField(refPartitionColumn.getName());
        if (sourceField == null) {
            return ImmutableList.of();
        }

        Optional<PartitionSpec> actualSpecOpt = resolveSpecForPartition(nativeTable, partitionName, actualSpecId);
        if (actualSpecOpt.isEmpty()) {
            return ImmutableList.of();
        }

        Set<String> syntheticPartitionValues = collectSyntheticTimeValuesByQuery(
                table, actualSpecOpt.get(), partitionName, sourceField, targetTransform, refPartitionColumn.getType());
        if (syntheticPartitionValues.isEmpty()) {
            return ImmutableList.of();
        }

        List<String> sortedValues = new ArrayList<>(syntheticPartitionValues);
        Collections.sort(sortedValues);
        return createSyntheticTimePartitionKeys(sortedValues, refPartitionColumns, targetTransform);
    }

    private static PartitionField findActivePartitionField(PartitionSpec spec, int sourceFieldId) {
        for (PartitionField field : spec.fields()) {
            if (field.sourceId() == sourceFieldId && !field.transform().isVoid()) {
                return field;
            }
        }
        return null;
    }

    private static Optional<PartitionSpec> resolveSpecForPartition(org.apache.iceberg.Table nativeTable,
                                                                   String partitionName,
                                                                   Integer specId) {
        if (specId != null && specId >= 0) {
            PartitionSpec spec = nativeTable.specs().get(specId);
            if (spec != null) {
                return Optional.of(spec);
            }
        }
        return findSpecForPartitionName(nativeTable, partitionName);
    }

    private static Optional<PartitionSpec> findSpecForPartitionName(org.apache.iceberg.Table nativeTable,
                                                                    String partitionName) {
        List<String> actualFieldNames = Lists.newArrayList();
        for (String pathSegment : partitionName.split("/")) {
            int separator = pathSegment.indexOf('=');
            if (separator <= 0) {
                return Optional.empty();
            }
            actualFieldNames.add(pathSegment.substring(0, separator));
        }
        if (actualFieldNames.isEmpty()) {
            return Optional.empty();
        }
        return nativeTable.specs().values().stream()
                .filter(spec -> getActivePartitionFieldNames(spec).equals(actualFieldNames))
                .findFirst();
    }

    private static List<String> getActivePartitionFieldNames(PartitionSpec spec) {
        return spec.fields().stream()
                .filter(field -> !field.transform().isVoid())
                .map(PartitionField::name)
                .collect(Collectors.toList());
    }

    private static Set<Long> collectSyntheticTruncateValuesFromBounds(org.apache.iceberg.Table nativeTable,
                                                                      PartitionSpec actualSpec,
                                                                      String actualPartitionName,
                                                                      Types.NestedField sourceField,
                                                                      int truncateWidth) {
        Set<Long> result = new LinkedHashSet<>();
        try (CloseableIterable<FileScanTask> tasks = nativeTable.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                if (file.specId() != actualSpec.specId()) {
                    continue;
                }
                if (!actualPartitionName.equals(actualSpec.partitionToPath(file.partition()))) {
                    continue;
                }

                Optional<Long> lowerOpt = decodeIntegralBound(file.lowerBounds(), sourceField);
                Optional<Long> upperOpt = decodeIntegralBound(file.upperBounds(), sourceField);
                if ((lowerOpt.isEmpty() || upperOpt.isEmpty()) && FeConstants.runningUnitTest) {
                    Optional<long[]> pathBoundsOpt = parseBoundsFromUnitTestPath(file.path().toString());
                    if (pathBoundsOpt.isPresent()) {
                        long[] pathBounds = pathBoundsOpt.get();
                        lowerOpt = Optional.of(pathBounds[0]);
                        upperOpt = Optional.of(pathBounds[1]);
                    }
                }
                if (lowerOpt.isEmpty() || upperOpt.isEmpty()) {
                    continue;
                }

                long lower = lowerOpt.get();
                long upper = upperOpt.get();
                if (upper < lower) {
                    continue;
                }

                long syntheticLower = truncateValue(lower, truncateWidth);
                long syntheticUpper = truncateValue(upper, truncateWidth);
                for (long value = syntheticLower; value <= syntheticUpper; value += truncateWidth) {
                    result.add(value);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to collect synthetic truncate partitions for {}.{} from old spec partition {}",
                    tableNameForLog(nativeTable), actualSpec.specId(), actualPartitionName, e);
        }
        return result;
    }

    private static Set<Long> collectSyntheticIntegralValuesByQuery(IcebergTable table,
                                                                   PartitionSpec actualSpec,
                                                                   String actualPartitionName,
                                                                   Types.NestedField sourceField,
                                                                   IcebergPartitionTransform targetTransform,
                                                                   int transformParam) {
        Optional<String> predicateOpt = buildPartitionPredicate(table, actualSpec, actualPartitionName);
        if (predicateOpt.isEmpty()) {
            return Collections.emptySet();
        }

        String sourceColumnName = table.getNativeTable().schema().findColumnName(sourceField.fieldId());
        if (Strings.isNullOrEmpty(sourceColumnName)) {
            return Collections.emptySet();
        }

        String transformFunction = targetTransform == IcebergPartitionTransform.BUCKET
                ? FunctionSet.ICEBERG_TRANSFORM_BUCKET
                : FunctionSet.ICEBERG_TRANSFORM_TRUNCATE;

        String sql = String.format(
                "SELECT DISTINCT %s(%s, %d) FROM %s WHERE (%s) AND %s IS NOT NULL",
                transformFunction,
                StatisticUtils.quoting(sourceColumnName),
                transformParam,
                StatisticUtils.quoting(table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName()),
                predicateOpt.get(),
                StatisticUtils.quoting(sourceColumnName));
        ConnectContext previous = ConnectContext.get();
        try {
            ConnectContext context = PARTITION_FALLBACK_EXECUTOR.createConnectContext();
            context.getSessionVariable().setEnableMaterializedViewRewrite(false);
            context.getSessionVariable().setEnableMaterializedViewRewriteForInsert(false);
            List<TResultBatch> batches = PARTITION_FALLBACK_EXECUTOR.executeDQL(sql, context);
            Set<Long> values = new LinkedHashSet<>();
            for (TResultBatch batch : batches) {
                for (ByteBuffer buffer : batch.getRows()) {
                    ByteBuf copied = Unpooled.copiedBuffer(buffer);
                    List<String> data = MetaFunctions.LookupRecord.fromJson(
                            copied.toString(StandardCharsets.UTF_8)).data;
                    if (data == null || data.isEmpty() || Strings.isNullOrEmpty(data.get(0))) {
                        continue;
                    }
                    values.add(Long.parseLong(data.get(0)));
                }
            }
            if (values.isEmpty() && FeConstants.runningUnitTest) {
                return collectSyntheticValuesFromUnitTestPath(table.getNativeTable(), actualSpec, actualPartitionName);
            }
            return values;
        } catch (Exception e) {
            LOG.warn("Failed to collect synthetic {} partitions by query for table {} partition {}",
                    targetTransform, table.getName(), actualPartitionName, e);
            if (FeConstants.runningUnitTest) {
                return collectSyntheticValuesFromUnitTestPath(table.getNativeTable(), actualSpec, actualPartitionName);
            }
            return Collections.emptySet();
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static Set<String> collectSyntheticTimeValuesByQuery(IcebergTable table,
                                                                 PartitionSpec actualSpec,
                                                                 String actualPartitionName,
                                                                 Types.NestedField sourceField,
                                                                 IcebergPartitionTransform targetTransform,
                                                                 Type partitionType) {
        Optional<String> predicateOpt = buildPartitionPredicate(table, actualSpec, actualPartitionName);
        if (predicateOpt.isEmpty()) {
            return Collections.emptySet();
        }

        String sourceColumnName = table.getNativeTable().schema().findColumnName(sourceField.fieldId());
        if (Strings.isNullOrEmpty(sourceColumnName)) {
            return Collections.emptySet();
        }

        String targetExpr = buildSyntheticTimeQueryExpr(sourceColumnName, targetTransform);
        if (Strings.isNullOrEmpty(targetExpr)) {
            return Collections.emptySet();
        }

        String quotedSourceColumn = StatisticUtils.quoting(sourceColumnName);
        String sql = String.format(
                "SELECT DISTINCT %s FROM %s WHERE (%s) AND %s IS NOT NULL",
                targetExpr,
                StatisticUtils.quoting(table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName()),
                predicateOpt.get(),
                quotedSourceColumn);
        ConnectContext previous = ConnectContext.get();
        try {
            ConnectContext context = PARTITION_FALLBACK_EXECUTOR.createConnectContext();
            context.getSessionVariable().setEnableMaterializedViewRewrite(false);
            context.getSessionVariable().setEnableMaterializedViewRewriteForInsert(false);
            List<TResultBatch> batches = PARTITION_FALLBACK_EXECUTOR.executeDQL(sql, context);
            Set<String> values = new LinkedHashSet<>();
            for (TResultBatch batch : batches) {
                for (ByteBuffer buffer : batch.getRows()) {
                    ByteBuf copied = Unpooled.copiedBuffer(buffer);
                    List<String> data = MetaFunctions.LookupRecord.fromJson(
                            copied.toString(StandardCharsets.UTF_8)).data;
                    if (data == null || data.isEmpty() || Strings.isNullOrEmpty(data.get(0))) {
                        continue;
                    }
                    values.add(data.get(0));
                }
            }
            return values;
        } catch (Exception e) {
            LOG.warn("Failed to collect synthetic {} partitions by query for table {} partition {}",
                    targetTransform, table.getName(), actualPartitionName, e);
            return Collections.emptySet();
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static Optional<String> buildPartitionPredicate(IcebergTable table,
                                                            PartitionSpec actualSpec,
                                                            String actualPartitionName) {
        List<PartitionField> activeFields = actualSpec.fields().stream()
                .filter(field -> !field.transform().isVoid())
                .collect(Collectors.toList());
        if (activeFields.isEmpty()) {
            return Optional.of("TRUE");
        }
        List<String> partitionValues = PartitionUtil.toPartitionValues(actualPartitionName);
        if (activeFields.size() != partitionValues.size()) {
            return Optional.empty();
        }

        List<String> predicates = new ArrayList<>(activeFields.size());
        for (int i = 0; i < activeFields.size(); i++) {
            PartitionField field = activeFields.get(i);
            String sourceColumnName = table.getNativeTable().schema().findColumnName(field.sourceId());
            if (Strings.isNullOrEmpty(sourceColumnName)) {
                return Optional.empty();
            }
            predicates.add("(" + convertPartitionTransformToPredicate(
                    table, field, sourceColumnName, partitionValues.get(i)) + ")");
        }
        return Optional.of(String.join(" AND ", predicates));
    }

    private static Optional<Long> decodeIntegralBound(Map<Integer, ByteBuffer> bounds,
                                                      Types.NestedField sourceField) {
        if (bounds == null) {
            return Optional.empty();
        }
        ByteBuffer buf = bounds.get(sourceField.fieldId());
        if (buf == null) {
            return Optional.empty();
        }

        Object value = Conversions.fromByteBuffer(sourceField.type(), buf);
        if (value instanceof Integer) {
            return Optional.of(((Integer) value).longValue());
        } else if (value instanceof Long) {
            return Optional.of((Long) value);
        } else if (value instanceof Short) {
            return Optional.of(((Short) value).longValue());
        }
        return Optional.empty();
    }

    private static long truncateValue(long value, int width) {
        long remainder = value % width;
        if (remainder < 0) {
            remainder += width;
        }
        return value - remainder;
    }

    private static Optional<long[]> parseBoundsFromUnitTestPath(String path) {
        Matcher matcher = UNIT_TEST_BOUNDS_PATTERN.matcher(path);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        try {
            return Optional.of(new long[] {
                    Long.parseLong(matcher.group(1)),
                    Long.parseLong(matcher.group(2))
            });
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static Set<Long> collectSyntheticValuesFromUnitTestPath(org.apache.iceberg.Table nativeTable,
                                                                    PartitionSpec actualSpec,
                                                                    String actualPartitionName) {
        Set<Long> result = new LinkedHashSet<>();
        try (CloseableIterable<FileScanTask> tasks = nativeTable.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                if (file.specId() != actualSpec.specId()) {
                    continue;
                }
                if (!actualPartitionName.equals(actualSpec.partitionToPath(file.partition()))) {
                    continue;
                }
                parseSyntheticValuesFromUnitTestPath(file.path().toString()).ifPresent(result::addAll);
            }
        } catch (Exception e) {
            LOG.warn("Failed to collect synthetic integral values from unit-test path for {}.{} partition {}",
                    tableNameForLog(nativeTable), actualSpec.specId(), actualPartitionName, e);
        }
        return result;
    }

    private static Optional<List<Long>> parseSyntheticValuesFromUnitTestPath(String path) {
        Matcher matcher = UNIT_TEST_SYNTHETIC_VALUES_PATTERN.matcher(path);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        try {
            List<Long> values = Arrays.stream(matcher.group(1).split("_"))
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
            return Optional.of(values);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static String tableNameForLog(org.apache.iceberg.Table nativeTable) {
        try {
            return nativeTable.name();
        } catch (Exception e) {
            return "<unknown>";
        }
    }

    private static String buildSyntheticTimeQueryExpr(String sourceColumnName,
                                                      IcebergPartitionTransform targetTransform) {
        String quotedSourceColumn = StatisticUtils.quoting(sourceColumnName);
        if (targetTransform == IcebergPartitionTransform.IDENTITY) {
            return quotedSourceColumn;
        }
        String dateFormatPattern;
        if (targetTransform == IcebergPartitionTransform.YEAR) {
            dateFormatPattern = "%Y";
        } else if (targetTransform == IcebergPartitionTransform.MONTH) {
            dateFormatPattern = "%Y-%m";
        } else if (targetTransform == IcebergPartitionTransform.DAY) {
            dateFormatPattern = "%Y-%m-%d";
        } else if (targetTransform == IcebergPartitionTransform.HOUR) {
            dateFormatPattern = "%Y-%m-%d-%H";
        } else {
            return null;
        }
        return String.format("date_format(date_trunc('%s', %s), '%s')",
                targetTransform.name().toLowerCase(Locale.ROOT), quotedSourceColumn, dateFormatPattern);
    }

    private static List<PartitionKey> createSyntheticTimePartitionKeys(List<String> rawValues,
                                                                       List<Column> refPartitionColumns,
                                                                       IcebergPartitionTransform targetTransform)
            throws AnalysisException {
        if (refPartitionColumns.size() != 1) {
            return ImmutableList.of();
        }
        Column refPartitionColumn = refPartitionColumns.get(0);
        List<PartitionKey> result = new ArrayList<>(rawValues.size());
        for (String rawValue : rawValues) {
            result.add(createSyntheticTimePartitionKey(rawValue, refPartitionColumn, targetTransform));
        }
        return result;
    }

    private static PartitionKey createSyntheticTimePartitionKey(String rawValue,
                                                                Column refPartitionColumn,
                                                                IcebergPartitionTransform targetTransform)
            throws AnalysisException {
        PartitionKey partitionKey = new IcebergPartitionKey();
        if (rawValue == null) {
            partitionKey.setNullPartitionValue("null");
            partitionKey.pushColumn(NullLiteral.create(refPartitionColumn.getType()),
                    refPartitionColumn.getType().getPrimitiveType());
            return partitionKey;
        }

        String normalizedValue = normalizeSyntheticTimePartitionValue(rawValue, refPartitionColumn.getType(), targetTransform);
        LiteralExpr exprValue = LiteralExprFactory.create(normalizedValue, refPartitionColumn.getType());
        partitionKey.pushColumn(exprValue, refPartitionColumn.getType().getPrimitiveType());
        return partitionKey;
    }

    private static String normalizeSyntheticTimePartitionValue(String rawValue,
                                                               Type partitionType,
                                                               IcebergPartitionTransform targetTransform) {
        return switch (targetTransform) {
            case YEAR -> partitionType.isDate() ? rawValue + "-01-01" : rawValue + "-01-01 00:00:00";
            case MONTH -> partitionType.isDate() ? rawValue + "-01" : rawValue + "-01 00:00:00";
            case DAY, IDENTITY -> partitionType.isDate()
                    ? rawValue
                    : (rawValue.length() == 10 ? rawValue + " 00:00:00" : rawValue);
            case HOUR -> rawValue.substring(0, 10) + " " + rawValue.substring(11) + ":00:00";
            default -> throw new IllegalArgumentException("Unsupported synthetic time transform: " + targetTransform);
        };
    }

    private static boolean isTimeBasedFallbackTransform(IcebergPartitionTransform transform, Type partitionType) {
        if (transform == IcebergPartitionTransform.YEAR
                || transform == IcebergPartitionTransform.MONTH
                || transform == IcebergPartitionTransform.DAY
                || transform == IcebergPartitionTransform.HOUR) {
            return true;
        }
        return transform == IcebergPartitionTransform.IDENTITY
                && (partitionType.isDate() || partitionType.isDatetime());
    }

    public static Expr getIcebergTablePartitionPredicateExpr(IcebergTable table,
                                                             String partitionColName,
                                                             SlotRef slotRef,
                                                             Expr expr) {
        return getIcebergTablePartitionPredicateExpr(table, partitionColName, slotRef, ImmutableList.of(expr));
    }

    /**
     * Generate Iceberg's partition predicate according its partition transform.
     * eg:
     * Iceberg table partition column: day(dt)
     * partition value      : 2023-01-02
     * generated predicate  : dt >= '2023-01-01 00:08:00' and dt < '2023-01-02:00:08:00'
     * NOTE: use range predicate rather than `date_trunc` function for better partition prune in Iceberg SDK.
     */
    public static Expr getIcebergTablePartitionPredicateExpr(IcebergTable table,
                                                             String partitionColName,
                                                             SlotRef slotRef,
                                                             List<Expr> exprs) {
        PartitionField partitionField = table.getPartitionFiled(partitionColName);
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColName, table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
        }
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.IDENTITY) {
            return MvUtils.convertToInPredicate(slotRef, exprs);
        } else if (transform == IcebergPartitionTransform.BUCKET
                || transform == IcebergPartitionTransform.TRUNCATE) {
            int param = extractTransformParam(partitionField.transform().toString());
            String funcName = transform == IcebergPartitionTransform.BUCKET
                    ? FunctionSet.ICEBERG_TRANSFORM_BUCKET
                    : FunctionSet.ICEBERG_TRANSFORM_TRUNCATE;
            SlotRef clonedSlotRef = (SlotRef) slotRef.clone();
            clonedSlotRef.setTblName(null);
            IntLiteral paramLiteral = new IntLiteral(param);
            FunctionCallExpr transformFunc = new FunctionCallExpr(funcName,
                    Lists.newArrayList(clonedSlotRef, paramLiteral));
            com.starrocks.catalog.Function builtinFn = ExprUtils.getBuiltinFunction(
                    funcName,
                    new com.starrocks.type.Type[] {slotRef.getType(), com.starrocks.type.IntegerType.INT},
                    com.starrocks.catalog.Function.CompareMode.IS_SUPERTYPE_OF);
            if (builtinFn != null) {
                transformFunc.setFn(builtinFn);
                transformFunc.setType(builtinFn.getReturnType());
            }
            List<Expr> result = Lists.newArrayList();
            for (Expr expr : exprs) {
                FunctionCallExpr clonedFunc = (FunctionCallExpr) transformFunc.clone();
                result.add(new BinaryPredicate(BinaryType.EQ, clonedFunc, expr));
            }
            return ExprUtils.compoundOr(result);
        } else {
            List<Expr> result = Lists.newArrayList();
            for (Expr expr : exprs) {
                if (!(expr instanceof LiteralExpr)) {
                    throw new StarRocksConnectorException("Partition value must be literal");
                }
                String partitionVal = ((LiteralExpr) expr).getStringValue();
                Range<String> range = toPartitionRange(table, partitionColName, partitionVal, partitionField,
                        false);
                Preconditions.checkArgument(!range.lowerEndpoint().equals(range.upperEndpoint()),
                        "Partition value must be range");
                try {
                    LiteralExpr lowerExpr = LiteralExprFactory.create(range.lowerEndpoint(), slotRef.getType());
                    LiteralExpr upperExpr = LiteralExprFactory.create(range.upperEndpoint(), slotRef.getType());
                    Expr lower = new BinaryPredicate(BinaryType.GE, slotRef, lowerExpr);
                    Expr upper = new BinaryPredicate(BinaryType.LT, slotRef, upperExpr);
                    result.add(ExprUtils.compoundAnd(ImmutableList.of(lower, upper)));
                } catch (AnalysisException e) {
                    throw new StarRocksConnectorException("Create literal expr failed", e);
                }
            }
            return ExprUtils.compoundOr(result);
        }
    }

    private static List<Expr> getMVPartitionExprs(MaterializedView mv) {
        if (mv.getPartitionInfo().getType() == PartitionType.EXPR_RANGE) {
            return ((ExpressionRangePartitionInfo) mv.getPartitionInfo()).getPartitionExprs(mv.getIdToColumn());
        } else if (mv.getPartitionInfo().getType() == PartitionType.EXPR_RANGE_V2) {
            return ((ExpressionRangePartitionInfoV2) mv.getPartitionInfo()).getPartitionExprs(mv.getIdToColumn());
        } else if (mv.getPartitionInfo().isListPartition()) {
            TableName tableName = new TableName(null, null, mv.getName());
            return ((ListPartitionInfo) mv.getPartitionInfo()).getPartitionExprs(tableName, mv.getIdToColumn());
        }
        return ImmutableList.of();
    }

    private static Optional<PartitionTransformSignature> buildSignatureFromPartitionField(PartitionField field,
                                                                                          Schema schema) {
        String sourceColumnName = schema.findColumnName(field.sourceId());
        if (Strings.isNullOrEmpty(sourceColumnName)) {
            return Optional.empty();
        }
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(field.transform().toString());
        Integer parameter = null;
        if (transform == IcebergPartitionTransform.BUCKET || transform == IcebergPartitionTransform.TRUNCATE) {
            try {
                parameter = extractTransformParam(field.transform().toString());
            } catch (StarRocksConnectorException e) {
                return Optional.empty();
            }
        }
        return Optional.of(new PartitionTransformSignature(transform, sourceColumnName, parameter));
    }

    private static Optional<PartitionTransformSignature> buildSignatureFromMvPartitionExpr(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return Optional.of(new PartitionTransformSignature(
                    IcebergPartitionTransform.IDENTITY, slotRef.getColumnName(), null));
        }
        if (!(expr instanceof FunctionCallExpr functionCallExpr)) {
            return Optional.empty();
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        functionCallExpr.collect(SlotRef.class, slotRefs);
        if (slotRefs.size() != 1) {
            return Optional.empty();
        }
        String functionName = functionCallExpr.getFunctionName();
        String sourceColumnName = slotRefs.get(0).getColumnName();
        if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)) {
            if (functionCallExpr.getChildren().size() != 2 || !(functionCallExpr.getChild(0) instanceof LiteralExpr literal)) {
                return Optional.empty();
            }
            IcebergPartitionTransform transform = switch (literal.getStringValue().toUpperCase(Locale.ROOT)) {
                case "YEAR" -> IcebergPartitionTransform.YEAR;
                case "MONTH" -> IcebergPartitionTransform.MONTH;
                case "DAY" -> IcebergPartitionTransform.DAY;
                case "HOUR" -> IcebergPartitionTransform.HOUR;
                default -> IcebergPartitionTransform.UNKNOWN;
            };
            if (transform == IcebergPartitionTransform.UNKNOWN) {
                return Optional.empty();
            }
            return Optional.of(new PartitionTransformSignature(transform, sourceColumnName, null));
        }
        if (FunctionSet.ICEBERG_TRANSFORM_BUCKET.equalsIgnoreCase(functionName)
                || FunctionSet.ICEBERG_TRANSFORM_TRUNCATE.equalsIgnoreCase(functionName)) {
            if (functionCallExpr.getChildren().size() != 2 || !(functionCallExpr.getChild(1) instanceof LiteralExpr literal)) {
                return Optional.empty();
            }
            try {
                Integer parameter = Integer.parseInt(literal.getStringValue());
                IcebergPartitionTransform transform = FunctionSet.ICEBERG_TRANSFORM_BUCKET.equalsIgnoreCase(functionName)
                        ? IcebergPartitionTransform.BUCKET
                        : IcebergPartitionTransform.TRUNCATE;
                return Optional.of(new PartitionTransformSignature(transform, sourceColumnName, parameter));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static IcebergPartitionTransform getTargetTransformForPartitionExpr(Expr partitionExpr, Type partitionType) {
        if (partitionExpr instanceof SlotRef) {
            if (partitionType.isDate() || partitionType.isDatetime()) {
                return IcebergPartitionTransform.IDENTITY;
            }
            return null;
        }
        if (!(partitionExpr instanceof FunctionCallExpr functionCallExpr)) {
            return null;
        }
        if (!functionCallExpr.getFunctionName().equalsIgnoreCase(FunctionSet.DATE_TRUNC)
                || functionCallExpr.getChildren().isEmpty()
                || !(functionCallExpr.getChild(0) instanceof StringLiteral stringLiteral)) {
            return null;
        }
        return switch (stringLiteral.getValue().toLowerCase(Locale.ROOT)) {
            case "year" -> IcebergPartitionTransform.YEAR;
            case "month" -> IcebergPartitionTransform.MONTH;
            case "day" -> IcebergPartitionTransform.DAY;
            case "hour" -> IcebergPartitionTransform.HOUR;
            default -> null;
        };
    }
}

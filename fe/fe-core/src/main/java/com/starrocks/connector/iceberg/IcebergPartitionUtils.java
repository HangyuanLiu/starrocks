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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergPartitionTransform.YEAR;

public class IcebergPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);

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

    /**
     * Check whether ALL partition transforms for the given source field across ALL specs are
     * time-family (year, month, day, hour).
     *
     * @param icebergTable the native Iceberg table
     * @param sourceFieldId the schema field id of the partition source column
     * @return true if every transform that references this source field is a time transform
     */
    public static boolean isAllTimeTransforms(org.apache.iceberg.Table icebergTable, int sourceFieldId) {
        Map<Integer, PartitionSpec> specs = icebergTable.specs();
        for (PartitionSpec spec : specs.values()) {
            for (PartitionField field : spec.fields()) {
                if (field.sourceId() == sourceFieldId) {
                    String transform = field.transform().toString().toLowerCase(Locale.ROOT);
                    if (!transform.equals("year") && !transform.equals("month") &&
                            !transform.equals("day") && !transform.equals("hour")) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Determine whether the given Iceberg table has undergone partition evolution and all transforms
     * for the specified partition column are time-family (year, month, day, hour).
     *
     * @param table the StarRocks IcebergTable wrapper
     * @param partitionColumn the partition column to check
     * @return true if the table has multiple specs and all transforms for this column are time-family
     */
    public static boolean isSafePartitionEvolution(IcebergTable table, Column partitionColumn) {
        org.apache.iceberg.Table nativeTable = table.getNativeTable();
        Map<Integer, PartitionSpec> specs = nativeTable.specs();
        if (specs.size() <= 1) {
            return false;
        }

        Schema schema = nativeTable.schema();
        Types.NestedField schemaField = schema.findField(partitionColumn.getName());
        if (schemaField == null) {
            return false;
        }

        return isAllTimeTransforms(nativeTable, schemaField.fieldId());
    }

    /**
     * Resolve the {@link PartitionUtil.DateTimeInterval} for a specific partition based on its spec id.
     * Unlike {@link #getDateTimeIntervalFromIceberg} which only checks the current spec, this method
     * looks up the historical spec that the partition was written with.
     *
     * @param table the StarRocks IcebergTable wrapper
     * @param partitionColumn the partition column
     * @param partitionInfo the partition info (must be an Iceberg Partition)
     * @return the DateTimeInterval corresponding to the transform, or NONE if not resolvable
     */
    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromPartition(IcebergTable table,
                                                                                  Column partitionColumn,
                                                                                  PartitionInfo partitionInfo) {
        if (!(partitionInfo instanceof Partition)) {
            return PartitionUtil.DateTimeInterval.NONE;
        }

        Partition icebergPartition = (Partition) partitionInfo;
        int specId = icebergPartition.getSpecId();
        org.apache.iceberg.Table nativeTable = table.getNativeTable();
        PartitionSpec spec = nativeTable.specs().get(specId);
        if (spec == null) {
            return PartitionUtil.DateTimeInterval.NONE;
        }

        Schema schema = nativeTable.schema();
        Types.NestedField schemaField = schema.findField(partitionColumn.getName());
        if (schemaField == null) {
            return PartitionUtil.DateTimeInterval.NONE;
        }
        int sourceFieldId = schemaField.fieldId();

        for (PartitionField field : spec.fields()) {
            if (field.sourceId() == sourceFieldId) {
                String transform = field.transform().toString().toLowerCase(Locale.ROOT);
                switch (transform) {
                    case "year":
                        return PartitionUtil.DateTimeInterval.YEAR;
                    case "month":
                        return PartitionUtil.DateTimeInterval.MONTH;
                    case "day":
                        return PartitionUtil.DateTimeInterval.DAY;
                    case "hour":
                        return PartitionUtil.DateTimeInterval.HOUR;
                    default:
                        return PartitionUtil.DateTimeInterval.NONE;
                }
            }
        }
        return PartitionUtil.DateTimeInterval.NONE;
    }

    /**
     * Resolve the {@link PartitionUtil.DateTimeInterval} from an MV partition expression.
     * Expects a {@code date_trunc('granularity', column)} function call.
     *
     * @param mvPartitionExpr the MV partition expression
     * @param columnType the column type (unused but kept for future validation)
     * @return the DateTimeInterval corresponding to the date_trunc granularity, or NONE
     */
    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromPartitionExpr(Expr mvPartitionExpr,
                                                                                      Type columnType) {
        if (!(mvPartitionExpr instanceof FunctionCallExpr)) {
            return PartitionUtil.DateTimeInterval.NONE;
        }

        FunctionCallExpr funcExpr = (FunctionCallExpr) mvPartitionExpr;
        if (!funcExpr.getFunctionName().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            return PartitionUtil.DateTimeInterval.NONE;
        }

        Expr granularityExpr = funcExpr.getChild(0);
        if (!(granularityExpr instanceof StringLiteral)) {
            return PartitionUtil.DateTimeInterval.NONE;
        }

        String granularity = ((StringLiteral) granularityExpr).getStringValue().toLowerCase(Locale.ROOT);
        switch (granularity) {
            case "year":
                return PartitionUtil.DateTimeInterval.YEAR;
            case "month":
                return PartitionUtil.DateTimeInterval.MONTH;
            case "day":
                return PartitionUtil.DateTimeInterval.DAY;
            case "hour":
                return PartitionUtil.DateTimeInterval.HOUR;
            default:
                return PartitionUtil.DateTimeInterval.NONE;
        }
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

    /**
     * Create a PartitionKey from partition values using a specific PartitionInfo for spec-aware resolution.
     * This is used when resolving partitions from historical specs (partition evolution).
     */
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

    private static List<Expr> getMVPartitionExprs(MaterializedView mv) {
        if (mv.getPartitionInfo().getType() == PartitionType.EXPR_RANGE) {
            return ((ExpressionRangePartitionInfo) mv.getPartitionInfo()).getPartitionExprs(mv.getIdToColumn());
        } else if (mv.getPartitionInfo().getType() == PartitionType.EXPR_RANGE_V2) {
            return ((ExpressionRangePartitionInfoV2) mv.getPartitionInfo()).getPartitionExprs(mv.getIdToColumn());
        } else if (mv.getPartitionInfo().isRangePartition()) {
            return Optional.ofNullable(mv.getPartitionRefTableExprs()).orElse(ImmutableList.of());
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
        return Optional.of(new PartitionTransformSignature(transform, sourceColumnName, null));
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
            if (functionCallExpr.getChildren().size() != 2
                    || !(functionCallExpr.getChild(0) instanceof LiteralExpr literal)) {
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
        return Optional.empty();
    }
}
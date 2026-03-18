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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.tvr.TvrDeltaStats;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.VarcharType;
import com.starrocks.type.VariantType;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.connector.iceberg.MIcebergTableMeta.MOCKED_ICEBERG_TABLES;
import static org.apache.iceberg.types.Types.NestedField.required;

public class MockIcebergMetadata implements ConnectorMetadata {
    private static final Map<String, Map<String, IcebergTableInfo>> MOCK_TABLE_MAP = new CaseInsensitiveMap<>();
    private final AtomicLong idGen = new AtomicLong(0L);
    public static final String MOCKED_ICEBERG_CATALOG_NAME = "iceberg0";
    public static final String MOCKED_UNPARTITIONED_DB_NAME = "unpartitioned_db";
    public static final String MOCKED_PARTITIONED_DB_NAME = "partitioned_db";
    public static final String MOCKED_PARTITIONED_TRANSFORMS_DB_NAME = "partitioned_transforms_db";

    public static final String MOCKED_UNPARTITIONED_TABLE_NAME0 = "t0";
    public static final String MOCKED_UNPARTITIONED_VARIANT_TABLE_NAME = "variant_t0";
    public static final String MOCKED_UNPARTITIONED_TABLE_NUMERIC = "t_numeric";
    public static final String MOCKED_PARTITIONED_TABLE_NAME1 = "t1";
    // date partition table
    public static final String MOCKED_PARTITIONED_TABLE_NAME2 = "t2";

    // string partition table
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME1 = "part_tbl1";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME2 = "part_tbl2";
    public static final String MOCKED_STRING_PARTITIONED_TABLE_NAME3 = "part_tbl3";

    // partition table with transforms
    public static final String MOCKED_PARTITIONED_YEAR_TABLE_NAME = "t0_year";
    public static final String MOCKED_PARTITIONED_MONTH_TABLE_NAME = "t0_month";
    public static final String MOCKED_PARTITIONED_DAY_TABLE_NAME = "t0_day";
    public static final String MOCKED_PARTITIONED_DAY_WITH_NULL_PARTITION_TABLE_NAME = "t0_day_with_null_partition";
    public static final String MOCKED_PARTITIONED_HOUR_TABLE_NAME = "t0_hour";
    public static final String MOCKED_PARTITIONED_BUCKET_TABLE_NAME = "t0_bucket";
    public static final String MOCKED_PARTITIONED_TRUNCATE_TABLE_NAME = "t0_truncate";
    // partition table with transforms and partition column type is timestamp with timezone
    public static final String MOCKED_PARTITIONED_YEAR_TZ_TABLE_NAME = "t0_year_tz";
    public static final String MOCKED_PARTITIONED_MONTH_TZ_TABLE_NAME = "t0_month_tz";
    public static final String MOCKED_PARTITIONED_DAY_TZ_TABLE_NAME = "t0_day_tz";
    public static final String MOCKED_PARTITIONED_HOUR_TZ_TABLE_NAME = "t0_hour_tz";
    // partition table with partition evolutions
    public static final String MOCKED_PARTITIONED_EVOLUTION_DATE_MONTH_IDENTITY_TABLE_NAME = "t0_date_month_identity_evolution";
    // MONTH→DAY time-family evolution (safe for T2-2 per-spec interval)
    public static final String MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME = "t0_month_to_day_evolution";
    public static final String MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME =
            "t0_month_to_truncate_evolution";
    public static final String MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME =
            "t0_day_to_bucket_evolution";
    public static final String MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME =
            "t0_bucket16_to_bucket32_evolution";

    private static final List<String> PARTITION_TABLE_NAMES = ImmutableList.of(MOCKED_PARTITIONED_TABLE_NAME1,
            MOCKED_PARTITIONED_TABLE_NAME2,
            MOCKED_STRING_PARTITIONED_TABLE_NAME1,
            MOCKED_STRING_PARTITIONED_TABLE_NAME2,
            MOCKED_STRING_PARTITIONED_TABLE_NAME3);

    private static final List<String> PARTITION_TRANSFORM_TABLE_NAMES =
            ImmutableList.of(MOCKED_PARTITIONED_YEAR_TABLE_NAME, MOCKED_PARTITIONED_MONTH_TABLE_NAME,
                    MOCKED_PARTITIONED_DAY_TABLE_NAME,
                    MOCKED_PARTITIONED_DAY_WITH_NULL_PARTITION_TABLE_NAME,
                    MOCKED_PARTITIONED_HOUR_TABLE_NAME,
                    MOCKED_PARTITIONED_BUCKET_TABLE_NAME,
                    MOCKED_PARTITIONED_TRUNCATE_TABLE_NAME,
                    MOCKED_PARTITIONED_YEAR_TZ_TABLE_NAME, MOCKED_PARTITIONED_MONTH_TZ_TABLE_NAME,
                    MOCKED_PARTITIONED_DAY_TZ_TABLE_NAME, MOCKED_PARTITIONED_HOUR_TZ_TABLE_NAME,
                    MOCKED_PARTITIONED_EVOLUTION_DATE_MONTH_IDENTITY_TABLE_NAME,
                    MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME,
                    MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME,
                    MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME,
                    MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME);

    private static final List<String> PARTITION_NAMES_0 = Lists.newArrayList("date=2020-01-01",
            "date=2020-01-02",
            "date=2020-01-03",
            "date=2020-01-04");
    private static final List<String> PARTITION_NAMES_1 = Lists.newArrayList("d=2023-08-01",
            "d=2023-08-02",
            "d=2023-08-03");
    private static final long PARTITION_INIT_VERSION = 100;

    public static String getStarRocksHome() throws IOException {
        String starRocksHome = System.getenv("STARROCKS_HOME");
        if (Strings.isNullOrEmpty(starRocksHome)) {
            starRocksHome = Files.createTempDirectory("STARROCKS_HOME").toAbsolutePath().toString();
        }
        return starRocksHome;
    }

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    static {
        try {
            mockUnPartitionedTable();
            mockPartitionedTable();
            mockPartitionTransforms();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Column> addMetaColumns(List<Column> originalColumns) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        builder.addAll(originalColumns);
        Column filePathColumn = new Column(IcebergTable.FILE_PATH, StringType.STRING, true);
        Column rowPositionColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT, true);
        filePathColumn.setIsHidden(true);
        rowPositionColumn.setIsHidden(true);
        builder.add(filePathColumn);
        builder.add(rowPositionColumn);
        return builder.build();
    }

    public static void mockUnPartitionedTable() throws IOException {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_UNPARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, IcebergTableInfo> icebergTableInfoMap = MOCK_TABLE_MAP.get(MOCKED_UNPARTITIONED_DB_NAME);

        registerUnpartitionedTable(icebergTableInfoMap, MOCKED_UNPARTITIONED_TABLE_NAME0,
                ImmutableList.of(new Column("id", IntegerType.INT, true),
                        new Column("data", StringType.STRING, true),
                        new Column("date", StringType.STRING, true)),
                new Schema(required(3, "id", Types.IntegerType.get()),
                        required(4, "data", Types.StringType.get()),
                        required(5, "date", Types.StringType.get())),
                3);
        registerUnpartitionedTable(icebergTableInfoMap, MOCKED_UNPARTITIONED_VARIANT_TABLE_NAME,
                ImmutableList.of(new Column("id", IntegerType.INT, true),
                        new Column("v", VariantType.VARIANT, true)),
                new Schema(required(3, "id", Types.IntegerType.get()),
                        required(4, "v", Types.VariantType.get())),
                3);
        registerUnpartitionedTable(icebergTableInfoMap, MOCKED_UNPARTITIONED_TABLE_NUMERIC,
                ImmutableList.of(new Column("id", IntegerType.INT, true),
                        new Column("c1", IntegerType.INT, true),
                        new Column("c2", IntegerType.INT, true)),
                new Schema(required(6, "id", Types.IntegerType.get()),
                        required(7, "c1", Types.IntegerType.get()),
                        required(8, "c2", Types.IntegerType.get())),
                3);
    }

    private static void registerUnpartitionedTable(Map<String, IcebergTableInfo> icebergTableInfoMap,
                                                   String tableName,
                                                   List<Column> schemas,
                                                   Schema schema,
                                                   int formatVersion) throws IOException {
        List<Column> fullSchemas = addMetaColumns(schemas);
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        TestTables.TestTable baseTable = TestTables.create(
                new File(getStarRocksHome() + "/" + MOCKED_UNPARTITIONED_DB_NAME + "/" + tableName),
                tableName, schema, spec, formatVersion);

        MockIcebergTable mockIcebergTable = new MockIcebergTable(tableName.hashCode(), tableName,
                MOCKED_ICEBERG_CATALOG_NAME, null, MOCKED_UNPARTITIONED_DB_NAME,
                tableName, fullSchemas, baseTable, null, "");

        List<String> colNames = fullSchemas.stream().map(Column::getName).collect(Collectors.toList());
        Map<String, ColumnStatistic> columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                col -> ColumnStatistic.unknown()));

        icebergTableInfoMap.put(tableName,
                new IcebergTableInfo(mockIcebergTable, Lists.newArrayList(), 100, columnStatisticMap));
    }

    public static void mockPartitionedTable() throws IOException {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, IcebergTableInfo> icebergTableInfoMap = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_DB_NAME);

        for (String tblName : PARTITION_TABLE_NAMES) {
            List<Column> columns = getPartitionedTableSchema(tblName);
            columns = addMetaColumns(columns);

            MockIcebergTable icebergTable = getPartitionIcebergTable(tblName, columns);
            Map<String, ColumnStatistic> columnStatisticMap;
            List<String> colNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                    col -> ColumnStatistic.unknown()));
            if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_0,
                        100, columnStatisticMap));
            } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_0,
                        100, columnStatisticMap));
            } else {
                icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, PARTITION_NAMES_1,
                        100, columnStatisticMap));
            }
        }
    }

    public static void mockPartitionTransforms() throws IOException {
        MOCK_TABLE_MAP.putIfAbsent(MOCKED_PARTITIONED_TRANSFORMS_DB_NAME, new CaseInsensitiveMap<>());
        Map<String, IcebergTableInfo> icebergTableInfoMap = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_TRANSFORMS_DB_NAME);

        List<String> allTableNames = Lists.newArrayList(PARTITION_TRANSFORM_TABLE_NAMES);
        MOCKED_ICEBERG_TABLES.keySet().stream().forEach(allTableNames::add);
        for (String tblName : allTableNames) {
            List<Column> columns = getPartitionedTransformTableSchema(tblName);
            MockIcebergTable icebergTable = getPartitionTransformIcebergTable(tblName, columns);
            List<String> partitionNames = getTransformTablePartitionNames(tblName);
            Map<String, ColumnStatistic> columnStatisticMap;
            List<String> colNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            columnStatisticMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                    col -> ColumnStatistic.unknown()));
            icebergTableInfoMap.put(tblName, new IcebergTableInfo(icebergTable, partitionNames,
                    100, columnStatisticMap));
        }
    }

    private static List<Column> getPartitionedTableSchema(String tblName) {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            return ImmutableList.of(new Column("id", IntegerType.INT, true),
                    new Column("data", StringType.STRING, true),
                    new Column("date", StringType.STRING, true));
        } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
            return ImmutableList.of(new Column("id", IntegerType.INT, true),
                    new Column("data", StringType.STRING, true),
                    new Column("date", DateType.DATE, true));
        } else {
            return Arrays.asList(new Column("a", VarcharType.VARCHAR), new Column("b", VarcharType.VARCHAR),
                    new Column("c", IntegerType.INT), new Column("d", VarcharType.VARCHAR));
        }
    }

    private static List<Column> getPartitionedTransformTableSchema(String tblName) {
        return ImmutableList.of(new Column("id", IntegerType.INT, true),
                new Column("data", StringType.STRING, true),
                new Column("ts", DateType.DATETIME, true));
    }

    private static Schema getIcebergPartitionSchema(String tblName) {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            return new Schema(required(3, "id", Types.IntegerType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "date", Types.StringType.get()));
        } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
            return new Schema(required(3, "id", Types.IntegerType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "date", Types.DateType.get()));
        } else {
            return new Schema(required(3, "a", Types.StringType.get()),
                    required(4, "b", Types.StringType.get()),
                    required(5, "c", Types.StringType.get()),
                    required(6, "d", Types.StringType.get()));
        }
    }

    private static Schema getIcebergPartitionTransformSchema(String tblName) {
        if (tblName.endsWith("tz")) {
            return new Schema(required(3, "id", Types.IntegerType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "ts", Types.TimestampType.withZone()));
        } else {
            return new Schema(required(3, "id", Types.IntegerType.get()),
                    required(4, "data", Types.StringType.get()),
                    required(5, "ts", Types.TimestampType.withoutZone()));
        }
    }

    private static TestTables.TestTable getPartitionIdentityTable(String tblName, Schema schema) throws IOException {
        if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME1)) {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema).identity("date").build();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_DB_NAME + "/"
                            + MOCKED_PARTITIONED_TABLE_NAME1), MOCKED_PARTITIONED_TABLE_NAME1,
                    schema, spec, 3);
        } else if (tblName.equals(MOCKED_PARTITIONED_TABLE_NAME2)) {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema).identity("date").build();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_DB_NAME + "/"
                            + MOCKED_PARTITIONED_TABLE_NAME2), MOCKED_PARTITIONED_TABLE_NAME2,
                    schema, spec, 1);
        } else {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema).identity("d").build();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + tblName + "/" + tblName),
                    tblName,
                    schema, spec, 1);
        }
    }

    private static TestTables.TestTable getPartitionTransformTable(String tblName, Schema schema) throws IOException {
        switch (tblName) {
            case MOCKED_PARTITIONED_YEAR_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).year("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_YEAR_TABLE_NAME), MOCKED_PARTITIONED_YEAR_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_MONTH_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).month("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_MONTH_TABLE_NAME), MOCKED_PARTITIONED_MONTH_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_DAY_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).day("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_DAY_TABLE_NAME), MOCKED_PARTITIONED_DAY_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_DAY_WITH_NULL_PARTITION_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).day("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_DAY_WITH_NULL_PARTITION_TABLE_NAME),
                        MOCKED_PARTITIONED_DAY_WITH_NULL_PARTITION_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_HOUR_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).hour("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_HOUR_TABLE_NAME), MOCKED_PARTITIONED_HOUR_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_BUCKET_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).bucket("ts", 10).build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_BUCKET_TABLE_NAME), MOCKED_PARTITIONED_BUCKET_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_TRUNCATE_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).truncate("data", 5).build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_TRUNCATE_TABLE_NAME), MOCKED_PARTITIONED_TRUNCATE_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_YEAR_TZ_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).year("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_YEAR_TZ_TABLE_NAME), MOCKED_PARTITIONED_YEAR_TZ_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_MONTH_TZ_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).month("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_MONTH_TZ_TABLE_NAME), MOCKED_PARTITIONED_MONTH_TZ_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_DAY_TZ_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).day("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_DAY_TZ_TABLE_NAME), MOCKED_PARTITIONED_DAY_TZ_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_HOUR_TZ_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).hour("ts").build();
                return TestTables.create(
                        new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                                + MOCKED_PARTITIONED_HOUR_TZ_TABLE_NAME), MOCKED_PARTITIONED_HOUR_TZ_TABLE_NAME,
                        schema, spec, 1);
            }
            case MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME: {
                // MONTH(ts) → DAY(ts) evolution: spec 0 = MONTH, spec 1 = DAY
                PartitionSpec specMonth =
                        PartitionSpec.builderFor(schema).month("ts").build();
                File fileEvol = new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                        + MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME);
                TestTables.TestTable tableEvol = TestTables.create(
                        fileEvol,
                        MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME,
                        schema, specMonth, 1);
                TableMetadata evolMeta = tableEvol.ops().current().updatePartitionSpec(
                        PartitionSpec.builderFor(tableEvol.ops().current().schema())
                                .day("ts").build());
                tableEvol.ops().commit(tableEvol.ops().current(), evolMeta);
                return tableEvol;
            }
            case MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME: {
                PartitionSpec specMonth =
                        PartitionSpec.builderFor(schema).month("ts").build();
                File fileEvol = new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                        + MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME);
                TestTables.TestTable tableEvol = TestTables.create(
                        fileEvol,
                        MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME,
                        schema, specMonth, 1);
                TableMetadata evolMeta = tableEvol.ops().current().updatePartitionSpec(
                        PartitionSpec.builderFor(tableEvol.ops().current().schema())
                                .truncate("id", 10).build());
                tableEvol.ops().commit(tableEvol.ops().current(), evolMeta);
                return tableEvol;
            }
            case MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME: {
                PartitionSpec specDay =
                        PartitionSpec.builderFor(schema).day("ts").build();
                File fileEvol = new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                        + MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME);
                TestTables.TestTable tableEvol = TestTables.create(
                        fileEvol,
                        MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME,
                        schema, specDay, 1);
                TableMetadata evolMeta = tableEvol.ops().current().updatePartitionSpec(
                        PartitionSpec.builderFor(tableEvol.ops().current().schema())
                                .bucket("id", 16).build());
                tableEvol.ops().commit(tableEvol.ops().current(), evolMeta);
                return tableEvol;
            }
            case MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME: {
                PartitionSpec specBucket16 =
                        PartitionSpec.builderFor(schema).bucket("id", 16).build();
                File fileEvol = new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                        + MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME);
                TestTables.TestTable tableEvol = TestTables.create(
                        fileEvol,
                        MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME,
                        schema, specBucket16, 1);
                TableMetadata evolMeta = tableEvol.ops().current().updatePartitionSpec(
                        PartitionSpec.builderFor(tableEvol.ops().current().schema())
                                .bucket("id", 32).build());
                tableEvol.ops().commit(tableEvol.ops().current(), evolMeta);
                return tableEvol;
            }
            case MOCKED_PARTITIONED_EVOLUTION_DATE_MONTH_IDENTITY_TABLE_NAME: {
                PartitionSpec spec =
                        PartitionSpec.builderFor(schema).month("ts").build();
                File file = new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                        + MOCKED_PARTITIONED_EVOLUTION_DATE_MONTH_IDENTITY_TABLE_NAME);

                TestTables.TestTable table = TestTables.create(
                        file,
                        MOCKED_PARTITIONED_EVOLUTION_DATE_MONTH_IDENTITY_TABLE_NAME,
                        schema, spec, 1);
                TableMetadata evolutionMetaData = table.ops().current().updatePartitionSpec(
                        PartitionSpec.builderFor(table.ops().current().schema()).identity("ts").build());

                table.ops().commit(table.ops().current(), evolutionMetaData);
                return table;
            }
            default: {
                if (MOCKED_ICEBERG_TABLES.containsKey(tblName)) {
                    MIcebergTable table = MOCKED_ICEBERG_TABLES.get(tblName);
                    return table.getTestTable(schema);
                }
            }
        }
        return null;
    }

    public static List<String> getTransformTablePartitionNames(String tblName) {
        switch (tblName) {
            case MOCKED_PARTITIONED_YEAR_TABLE_NAME:
            case MOCKED_PARTITIONED_YEAR_TZ_TABLE_NAME:
                return Lists.newArrayList("ts_year=2019", "ts_year=2020",
                        "ts_year=2021", "ts_year=2022", "ts_year=2023");
            case MOCKED_PARTITIONED_MONTH_TABLE_NAME:
            case MOCKED_PARTITIONED_MONTH_TZ_TABLE_NAME:
                return Lists.newArrayList("ts_month=2022-01", "ts_month=2022-02",
                        "ts_month=2022-03", "ts_month=2022-04", "ts_month=2022-05");
            case MOCKED_PARTITIONED_DAY_TABLE_NAME:
            case MOCKED_PARTITIONED_DAY_TZ_TABLE_NAME:
                return Lists.newArrayList("ts_day=2022-01-01", "ts_day=2022-01-02",
                        "ts_day=2022-01-03", "ts_day=2022-01-04", "ts_day=2022-01-05");
            case MOCKED_PARTITIONED_DAY_WITH_NULL_PARTITION_TABLE_NAME:
                return Lists.newArrayList("ts_day=2022-01-01", "ts_day=2022-01-02",
                        "ts_day=2022-01-03", "ts_day=2022-01-04", "ts_day=2022-01-05", "ts_day=null");
            case MOCKED_PARTITIONED_HOUR_TABLE_NAME:
            case MOCKED_PARTITIONED_HOUR_TZ_TABLE_NAME:
                return Lists.newArrayList("ts_hour=2022-01-01-00", "ts_hour=2022-01-01-01",
                        "ts_hour=2022-01-01-02", "ts_hour=2022-01-01-03", "ts_hour=2022-01-01-04");
            case MOCKED_PARTITIONED_BUCKET_TABLE_NAME:
                return Lists.newArrayList("ts_bucket=0", "ts_bucket=1",
                        "ts_bucket=2", "ts_bucket=3", "ts_bucket=4");
            case MOCKED_PARTITIONED_TRUNCATE_TABLE_NAME:
                return Lists.newArrayList("data_trunc=aaaaa", "data_trunc=bbbbb",
                        "data_trunc=ccccc", "data_trunc=ddddd", "data_trunc=eeeee");
            case MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME:
                // specId=0 (MONTH): 2 month partitions; specId=1 (DAY): 3 day partitions
                return Lists.newArrayList(
                        "ts_month=2024-01", "ts_month=2024-02",
                        "ts_day=2024-03-01", "ts_day=2024-03-02", "ts_day=2024-03-03");
            case MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME:
                return Lists.newArrayList(
                        "ts_month=2024-01", "ts_month=2024-02",
                        "id_trunc=0", "id_trunc=10");
            case MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME:
                return Lists.newArrayList(
                        "ts_day=2024-01-01", "ts_day=2024-01-02", "ts_day=2024-01-03");
            case MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME:
                return Lists.newArrayList("id_bucket=1", "id_bucket=2", "id_bucket=3");
            case MOCKED_PARTITIONED_EVOLUTION_DATE_MONTH_IDENTITY_TABLE_NAME:
                return Lists.newArrayList("ts=2024-01-01", "ts_month=2024-01",
                        "ts=2024-02", "ts=2024-03");
            default: {
                if (MOCKED_ICEBERG_TABLES.containsKey(tblName)) {
                    MIcebergTable table = MOCKED_ICEBERG_TABLES.get(tblName);
                    return table.getTransformTablePartitionNames(tblName);
                }
            }
        }
        return null;
    }

    public static MockIcebergTable getPartitionIcebergTable(String tblName, List<Column> schemas) throws IOException {
        Schema schema = getIcebergPartitionSchema(tblName);
        TestTables.TestTable baseTable = getPartitionIdentityTable(tblName, schema);

        return new MockIcebergTable(tblName.hashCode(), tblName, MOCKED_ICEBERG_CATALOG_NAME,
                null, MOCKED_PARTITIONED_DB_NAME, tblName, schemas, baseTable, null, "");
    }

    public static MockIcebergTable getPartitionTransformIcebergTable(String tblName, List<Column> schemas)
            throws IOException {
        Schema schema = getIcebergPartitionTransformSchema(tblName);
        TestTables.TestTable baseTable = getPartitionTransformTable(tblName, schema);

        return new MockIcebergTable(tblName.hashCode(), tblName, MOCKED_ICEBERG_CATALOG_NAME,
                null, MOCKED_PARTITIONED_TRANSFORMS_DB_NAME, tblName, schemas, baseTable, null, "");
    }

    @Override
    public com.starrocks.catalog.Table.TableType getTableType() {
        return ICEBERG;
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
    }

    @Override
    public com.starrocks.catalog.Table getTable(ConnectContext context, String dbName, String tblName) {
        readLock();
        try {
            Map<String, IcebergTableInfo> dbTables = MOCK_TABLE_MAP.get(dbName);
            if (dbTables == null) {
                return getView(context, dbName, tblName);
            }
            IcebergTableInfo tableInfo = dbTables.get(tblName);
            if (tableInfo == null) {
                return getView(context, dbName, tblName);
            }
            MockIcebergTable t = tableInfo.icebergTable;
            MockIcebergTable t1 = new MockIcebergTable(t.getId(), t.getName(), t.getCatalogName(), t.getResourceName(),
                    t.getCatalogDBName(), t.getCatalogTableName(),
                    t.getBaseSchema(), t.getNativeTable(), t.getIcebergProperties(),
                    t.getComment());
            ConnectorTableInfo info = GlobalStateMgr.getCurrentState()
                    .getConnectorTblMetaInfoMgr()
                    .getConnectorTableInfo(t.getCatalogName(), t.getCatalogDBName(), t.getTableIdentifier());
            if (info != null && info.getRelatedMaterializedViews() != null) {
                t1.getRelatedMaterializedViews().addAll(info.getRelatedMaterializedViews());
            }
            return t1;
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, ConnectorMetadatRequestContext requestContext) {
        readLock();
        try {
            return MOCK_TABLE_MAP.get(dbName).get(tableName).partitionNames;
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(com.starrocks.catalog.Table table, List<String> partitionNames) {
        IcebergTable icebergTable = (IcebergTable) table;
        readLock();
        try {
            Map<String, PartitionInfo> partitionInfoMap = MOCK_TABLE_MAP.get(icebergTable.getCatalogDBName()).
                    get(icebergTable.getCatalogTableName()).partitionInfoMap;
            if (icebergTable.isUnPartitioned()) {
                return Lists.newArrayList(partitionInfoMap.get(icebergTable.getCatalogTableName()));
            } else {
                return partitionNames.stream().map(partitionInfoMap::get).collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
    }

    public TvrTableSnapshot getCurrentTvrSnapshot(String dbName, com.starrocks.catalog.Table table) {
        return TvrTableSnapshot.of(TvrVersion.of(1L));
    }

    public List<TvrTableDeltaTrait> listTableDeltaTraits(String dbName, com.starrocks.catalog.Table table,
                                                         TvrTableSnapshot fromSnapshotExclusive,
                                                         TvrTableSnapshot toSnapshotInclusive) {
        TvrVersionRange currentRange = getCurrentTvrSnapshot(dbName, table);
        TvrTableDeltaTrait delta = TvrTableDeltaTrait.ofMonotonic(
                TvrTableDelta.of(TvrVersion.of(1L), currentRange.to),
                TvrDeltaStats.EMPTY);
        return Lists.newArrayList(delta);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, com.starrocks.catalog.Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit, TvrVersionRange version) {
        MockIcebergTable icebergTable = (MockIcebergTable) table;
        String hiveDb = icebergTable.getCatalogDBName();
        String tblName = icebergTable.getName();

        readLock();
        try {
            IcebergTableInfo info = MOCK_TABLE_MAP.get(hiveDb).get(tblName);
            Statistics.Builder builder = Statistics.builder();
            builder.setOutputRowCount(info.rowCount);
            for (ColumnRefOperator columnRefOperator : columns.keySet()) {
                ColumnStatistic columnStatistic = info.columnStatsMap.get(columnRefOperator.getName());
                if (columnStatistic == null) {
                    columnStatistic = ColumnStatistic.unknown();
                }
                builder.addColumnStatistic(columnRefOperator, columnStatistic);
            }
            return builder.build();
        } finally {
            readUnlock();
        }
    }

    public void addRowsToPartition(String dbName, String tableName, int rowCount, String partitionName) {
        addRowsToPartitionInternal(dbName, tableName, rowCount, partitionName, null, null, null, null);
    }

    public void addRowsToPartitionWithBounds(String dbName, String tableName, int rowCount,
                                             String partitionName, int lowerBound, int upperBound) {
        addRowsToPartitionInternal(dbName, tableName, rowCount, partitionName, null, lowerBound, upperBound, null);
    }

    public void addRowsToPartitionWithSyntheticValues(String dbName, String tableName, int rowCount,
                                                      String partitionName, int specId, List<Integer> syntheticValues) {
        addRowsToPartitionInternal(dbName, tableName, rowCount, partitionName, specId, null, null, syntheticValues);
    }

    private void addRowsToPartitionInternal(String dbName, String tableName, int rowCount, String partitionName,
                                            Integer specId,
                                            Integer lowerBound,
                                            Integer upperBound,
                                            List<Integer> syntheticValues) {
        IcebergTableInfo tableInfo = MOCK_TABLE_MAP.get(dbName).get(tableName);
        IcebergTable icebergTable = tableInfo.icebergTable;
        Table nativeTable = icebergTable.getNativeTable();
        PartitionSpec spec = resolvePartitionSpec(nativeTable, partitionName, specId);
        String path = String.format("/path/to/%s-%s%s-%d.parquet",
                tableName,
                partitionName.replace('/', '_'),
                buildSyntheticPathSuffix(lowerBound, upperBound, syntheticValues),
                System.nanoTime());

        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(10)
                .withPartition(IcebergPartitionData.partitionDataFromPath(partitionName, spec))
                .withRecordCount(rowCount);
        if (lowerBound != null && upperBound != null) {
            int fieldId = nativeTable.schema().findField("id").fieldId();
            builder.withMetrics(new Metrics(
                    (long) rowCount,
                    null,
                    null,
                    null,
                    null,
                    ImmutableMap.of(fieldId, Conversions.toByteBuffer(Types.IntegerType.get(), lowerBound)),
                    ImmutableMap.of(fieldId, Conversions.toByteBuffer(Types.IntegerType.get(), upperBound))));
        }
        DataFile file = builder.build();

        writeLock();
        try {
            nativeTable.newAppend().appendFile(file).commit();
            tableInfo.touchPartition(partitionName, specId);
        } finally {
            writeUnlock();
        }
    }

    private String buildSyntheticPathSuffix(Integer lowerBound, Integer upperBound, List<Integer> syntheticValues) {
        StringBuilder suffix = new StringBuilder();
        if (lowerBound != null && upperBound != null) {
            suffix.append(String.format("-lb%d-ub%d", lowerBound, upperBound));
        }
        if (syntheticValues != null && !syntheticValues.isEmpty()) {
            suffix.append("-sv").append(syntheticValues.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining("_")));
        }
        return suffix.toString();
    }

    private PartitionSpec resolvePartitionSpec(Table nativeTable, String partitionName, Integer specId) {
        if (specId != null && nativeTable.specs().containsKey(specId)) {
            return nativeTable.specs().get(specId);
        }
        List<String> actualFieldNames = Arrays.stream(partitionName.split("/"))
                .map(part -> part.split("=", 2)[0])
                .collect(Collectors.toList());
        for (PartitionSpec spec : nativeTable.specs().values()) {
            List<String> specFieldNames = spec.fields().stream()
                    .filter(field -> !field.transform().isVoid())
                    .map(PartitionField::name)
                    .collect(Collectors.toList());
            if (specFieldNames.equals(actualFieldNames)) {
                return spec;
            }
        }

        Schema schema = nativeTable.schema();
        if (partitionName.startsWith("ts_month=")) {
            return PartitionSpec.builderFor(schema).month("ts").build();
        } else if (partitionName.startsWith("ts_day=")) {
            return PartitionSpec.builderFor(schema).day("ts").build();
        } else if (partitionName.startsWith("ts_year=")) {
            return PartitionSpec.builderFor(schema).year("ts").build();
        } else if (partitionName.startsWith("ts_hour=")) {
            return PartitionSpec.builderFor(schema).hour("ts").build();
        }
        return nativeTable.spec();
    }

    public void updatePartitions(String dbName, String tableName, List<String> partitionNames) {
        writeLock();
        try {
            IcebergTableInfo tableInfo = MOCK_TABLE_MAP.get(dbName).get(tableName);
            for (String partitionName : partitionNames) {
                tableInfo.touchPartition(partitionName);
            }
        } finally {
            writeUnlock();
        }
    }

    private int resolveSpecId(String tableName, String partitionName) {
        IcebergTableInfo tableInfo = MOCK_TABLE_MAP.get(MOCKED_PARTITIONED_TRANSFORMS_DB_NAME).get(tableName);
        if (tableInfo == null) {
            return 0;
        }
        return tableInfo.resolveSpecId(tableName, partitionName);
    }

    private static class IcebergTableInfo {
        private MockIcebergTable icebergTable;
        private final List<String> partitionNames;
        private final Map<String, PartitionInfo> partitionInfoMap;
        private final long rowCount;
        private final Map<String, ColumnStatistic> columnStatsMap;

        public IcebergTableInfo(MockIcebergTable icebergTable, List<String> partitionNames,
                                long rowCount, Map<String, ColumnStatistic> columnStatsMap) {
            this.icebergTable = icebergTable;
            this.partitionNames = partitionNames;
            this.partitionInfoMap = Maps.newHashMap();
            this.rowCount = rowCount;
            this.columnStatsMap = columnStatsMap;
            initPartitionInfos(partitionNames);
        }

        private void initPartitionInfos(List<String> partitionNames) {
            if (partitionNames.isEmpty()) {
                partitionInfoMap.put(icebergTable.getCatalogTableName(), new Partition(PARTITION_INIT_VERSION));
            } else {
                String tblName = icebergTable.getCatalogTableName();
                for (String partitionName : partitionNames) {
                    int specId = resolveSpecId(tblName, partitionName);
                    partitionInfoMap.put(partitionName,
                            new Partition(PARTITION_INIT_VERSION, specId));
                }
            }
        }

        private void touchPartition(String partitionName) {
            touchPartition(partitionName, null);
        }

        private void touchPartition(String partitionName, Integer explicitSpecId) {
            int specId = explicitSpecId != null
                    ? explicitSpecId
                    : resolveSpecId(icebergTable.getCatalogTableName(), partitionName);
            if (!partitionNames.contains(partitionName)) {
                partitionNames.add(partitionName);
            }
            if (partitionInfoMap.containsKey(partitionName)) {
                long modifyTime = partitionInfoMap.get(partitionName).getModifiedTime() + 1;
                partitionInfoMap.put(partitionName, new Partition(modifyTime, specId));
            } else {
                partitionInfoMap.put(partitionName, new Partition(PARTITION_INIT_VERSION, specId));
            }
        }

        /**
         * Resolve specId for a partition. For evolution tables, look up the actual
         * specId from the native Iceberg table metadata by matching the partition name
         * prefix to the transform type.
         */
        private int resolveSpecId(String tblName, String partitionName) {
            if (icebergTable == null) {
                return 0;
            }
            if (!MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME.equals(tblName)
                    && !MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_TRUNCATE_TABLE_NAME.equals(tblName)
                    && !MOCKED_PARTITIONED_EVOLUTION_DAY_TO_BUCKET_TABLE_NAME.equals(tblName)
                    && !MOCKED_PARTITIONED_EVOLUTION_BUCKET16_TO_BUCKET32_TABLE_NAME.equals(tblName)) {
                return 0;
            }
            try {
                org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
                if (nativeTable == null || nativeTable.specs().size() <= 1) {
                    return 0;
                }
                List<String> actualFieldNames = Arrays.stream(partitionName.split("/"))
                        .map(part -> part.split("=", 2)[0])
                        .collect(Collectors.toList());
                if (actualFieldNames.isEmpty()) {
                    return 0;
                }
                for (Map.Entry<Integer, PartitionSpec> entry : nativeTable.specs().entrySet()) {
                    List<String> specFieldNames = entry.getValue().fields().stream()
                            .filter(field -> !field.transform().isVoid())
                            .map(PartitionField::name)
                            .collect(Collectors.toList());
                    if (specFieldNames.equals(actualFieldNames)) {
                        return entry.getKey();
                    }
                }
            } catch (Exception e) {
                // fallback
            }
            return 0;
        }
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    public com.starrocks.catalog.Table getView(ConnectContext context, String dbName, String viewName) {
        // Return a mock IcebergView for testing
        if (dbName.equalsIgnoreCase("view_db") && viewName.equalsIgnoreCase("iceberg_view")) {
            List<Column> schema = Lists.newArrayList(
                    new Column("id", IntegerType.INT),
                    new Column("data", VarcharType.VARCHAR),
                    new Column("date", DateType.DATE)
            );
            return new IcebergView(1, MOCKED_ICEBERG_CATALOG_NAME, dbName, viewName, schema,
                    "SELECT 1 as id, 'data' as data, CAST('2024-01-01' as DATE) as date", MOCKED_ICEBERG_CATALOG_NAME, dbName,
                    "view_location", Maps.newHashMap());
        }
        return null;
    }
}

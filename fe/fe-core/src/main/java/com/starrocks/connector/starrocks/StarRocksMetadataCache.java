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

package com.starrocks.connector.starrocks;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TQueryPlanInfo;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TTabletVersionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Metadata cache for the StarRocks external connector. It orchestrates JDBC metadata fetches, REST
 * `_query_plan` requests and retains them with short TTL to reduce pressure on the remote cluster.
 */
public class StarRocksMetadataCache implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(StarRocksMetadataCache.class);

    private static final long DEFAULT_DATABASE_TTL_MILLIS = 60_000L;
    private static final long DEFAULT_TABLE_LIST_TTL_MILLIS = 60_000L;
    private static final long DEFAULT_TABLE_META_TTL_MILLIS = 300_000L;

    private final ConnectorContext context;
    private final StarRocksConnectorConfig config;
    private final StarRocksJdbcClient jdbcClient;
    private final StarRocksRestClient restClient;
    private final Clock clock;
    private final long databaseTtlMillis;
    private final long tableListTtlMillis;
    private final long tableMetaTtlMillis;

    private volatile CacheEntry<List<String>> cachedDatabases;
    private final Object databaseLock = new Object();

    private final ConcurrentMap<String, CacheEntry<List<String>>> tableListCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableCacheKey, CacheEntry<StarRocksExternalTable>> tableCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableCacheKey, ConnectorTableId> tableIdCache = new ConcurrentHashMap<>();

    public StarRocksMetadataCache(ConnectorContext context,
                                  StarRocksConnectorConfig config) {
        this(context, config,
                StarRocksJdbcClient.create(config),
                StarRocksRestClient.create(config),
                Clock.systemUTC(),
                DEFAULT_DATABASE_TTL_MILLIS,
                DEFAULT_TABLE_LIST_TTL_MILLIS,
                DEFAULT_TABLE_META_TTL_MILLIS);
    }

    StarRocksMetadataCache(ConnectorContext context,
                           StarRocksConnectorConfig config,
                           StarRocksJdbcClient jdbcClient,
                           StarRocksRestClient restClient,
                           Clock clock,
                           long databaseTtlMillis,
                           long tableListTtlMillis,
                           long tableMetaTtlMillis) {
        this.context = Objects.requireNonNull(context, "context is null");
        this.config = Objects.requireNonNull(config, "config is null");
        this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient is null");
        this.restClient = Objects.requireNonNull(restClient, "restClient is null");
        this.clock = Objects.requireNonNull(clock, "clock is null");
        this.databaseTtlMillis = databaseTtlMillis;
        this.tableListTtlMillis = tableListTtlMillis;
        this.tableMetaTtlMillis = tableMetaTtlMillis;
    }

    public List<String> listDatabases() {
        CacheEntry<List<String>> snapshot = cachedDatabases;
        long now = clock.millis();
        if (snapshot != null && snapshot.isValid(now)) {
            return snapshot.value;
        }
        synchronized (databaseLock) {
            snapshot = cachedDatabases;
            if (snapshot != null && snapshot.isValid(now)) {
                return snapshot.value;
            }
            List<String> databases = jdbcClient.listDatabases();
            CacheEntry<List<String>> entry = new CacheEntry<>(ImmutableList.copyOf(databases), now + databaseTtlMillis);
            cachedDatabases = entry;
            return entry.value;
        }
    }

    public List<String> listTables(String dbName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName), "dbName is null or empty");
        String cacheKey = dbName.toLowerCase(Locale.ROOT);
        long now = clock.millis();
        CacheEntry<List<String>> entry = tableListCache.get(cacheKey);
        if (entry != null && entry.isValid(now)) {
            return entry.value;
        }
        return tableListCache.compute(cacheKey, (key, current) -> {
            long currentTime = clock.millis();
            if (current != null && current.isValid(currentTime)) {
                return current;
            }
            List<String> tables = jdbcClient.listTables(dbName);
            LOG.debug("Loaded {} tables for database '{}' in catalog '{}'", tables.size(), dbName, context.getCatalogName());
            return new CacheEntry<>(ImmutableList.copyOf(tables), currentTime + tableListTtlMillis);
        }).value;
    }

    public StarRocksExternalTable getTable(String dbName, String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dbName), "dbName is null or empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName is null or empty");
        TableCacheKey key = TableCacheKey.of(dbName, tableName);
        long now = clock.millis();
        CacheEntry<StarRocksExternalTable> cached = tableCache.get(key);
        if (cached != null && cached.isValid(now)) {
            return cached.value;
        }
        return tableCache.compute(key, (ignored, current) -> {
            long currentTime = clock.millis();
            if (current != null && current.isValid(currentTime)) {
                return current;
            }
            StarRocksExternalTable table = loadTableMetadata(key, currentTime);
            return new CacheEntry<>(table, currentTime + tableMetaTtlMillis);
        }).value;
    }

    public void invalidateTable(String dbName, String tableName) {
        tableCache.remove(TableCacheKey.of(dbName, tableName));
    }

    private StarRocksExternalTable loadTableMetadata(TableCacheKey key, long loadTimestamp) {
        String sql = buildSelectAllSql(key);
        StarRocksRestClient.QueryPlanResponse restResponse =
                restClient.getQueryPlan(key.dbName, key.tableName, sql);
        if (Strings.isNullOrEmpty(restResponse.getOpaquedQueryPlan())) {
            throw new StarRocksConnectorException("Empty query plan returned for table "
                    + qualifiedName(key));
        }
        TQueryPlanInfo planInfo = decodePlan(restResponse.getOpaquedQueryPlan());
        List<Column> columns = extractColumns(planInfo);
        if (columns.isEmpty()) {
            throw new StarRocksConnectorException("No columns resolved from query plan for table "
                    + qualifiedName(key));
        }
        Map<Long, StarRocksExternalTable.Tablet> tablets = assembleTablets(restResponse, planInfo);
        ConnectorTableId tableId = tableIdCache.computeIfAbsent(key,
                ignored -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId());
        return new StarRocksExternalTable(
                tableId,
                context.getCatalogName(),
                key.dbName,
                key.tableName,
                columns,
                restResponse.getOpaquedQueryPlan(),
                tablets,
                loadTimestamp,
                buildExecutionProperties(config));
    }

    private String qualifiedName(TableCacheKey key) {
        return context.getCatalogName() + "." + key.dbName + "." + key.tableName;
    }

    private static String buildSelectAllSql(TableCacheKey key) {
        return String.format(Locale.ROOT, "SELECT * FROM %s.%s", quote(key.dbName), quote(key.tableName));
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private TQueryPlanInfo decodePlan(String encodedPlan) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encodedPlan);
            TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
            TQueryPlanInfo info = new TQueryPlanInfo();
            deserializer.deserialize(info, bytes);
            return info;
        } catch (IllegalArgumentException e) {
            throw new StarRocksConnectorException("Failed to decode opaqued query plan", e);
        } catch (TException e) {
            throw new StarRocksConnectorException("Failed to deserialize query plan", e);
        }
    }

    private List<Column> extractColumns(TQueryPlanInfo planInfo) {
        TDescriptorTable descriptorTable = planInfo.getDesc_tbl();
        if (descriptorTable == null || descriptorTable.getSlotDescriptors() == null) {
            throw new StarRocksConnectorException("Descriptor table missing in query plan");
        }
        List<TSlotDescriptor> slotDescriptors = descriptorTable.getSlotDescriptors().stream()
                .filter(TSlotDescriptor::isIsOutputColumn)
                .filter(TSlotDescriptor::isSetColName)
                .sorted(Comparator
                        .comparingInt((TSlotDescriptor slot) -> slot.isSetSlotIdx() ? slot.getSlotIdx() : Integer.MAX_VALUE)
                        .thenComparingInt(slot -> slot.isSetColumnPos() ? slot.getColumnPos() : Integer.MAX_VALUE))
                .collect(Collectors.toList());
        List<Column> columns = new ArrayList<>(slotDescriptors.size());
        for (TSlotDescriptor slot : slotDescriptors) {
            if (slot.getSlotType() == null) {
                throw new StarRocksConnectorException("Slot type missing for column " + slot.getColName());
            }
            Type type = Type.fromThrift(slot.getSlotType());
            Column column = new Column(slot.getColName(), type, slot.isIsNullable());
            if (slot.isSetCol_unique_id()) {
                column.setUniqueId(slot.getCol_unique_id());
            }
            columns.add(column);
        }
        return columns;
    }

    private Map<Long, StarRocksExternalTable.Tablet> assembleTablets(
            StarRocksRestClient.QueryPlanResponse restResponse,
            TQueryPlanInfo planInfo) {
        Map<Long, StarRocksRestClient.QueryPlanResponse.TabletRouting> routing =
                restResponse.getTablets();
        Map<Long, TTabletVersionInfo> versionInfo = planInfo.getTablet_info();
        if ((routing == null || routing.isEmpty()) && (versionInfo == null || versionInfo.isEmpty())) {
            return ImmutableMap.of();
        }
        Map<Long, StarRocksExternalTable.Tablet> tablets = Maps.newHashMap();
        if (versionInfo != null) {
            versionInfo.forEach((tabletId, info) -> {
                StarRocksRestClient.QueryPlanResponse.TabletRouting route =
                        routing != null ? routing.get(tabletId) : null;
                long version = info.isSetVersion() ? info.getVersion()
                        : (route != null ? route.getVersion() : -1L);
                int schemaHash = info.isSetSchema_hash() ? info.getSchema_hash()
                        : (route != null ? route.getSchemaHash() : 0);
                List<String> endpoints = route != null ? route.getEndpoints() : ImmutableList.of();
                tablets.put(tabletId, new StarRocksExternalTable.Tablet(tabletId, version, schemaHash, endpoints));
            });
        }
        if (routing != null) {
            routing.forEach((tabletId, route) -> tablets.computeIfAbsent(
                    tabletId,
                    id -> new StarRocksExternalTable.Tablet(
                            tabletId,
                            route.getVersion(),
                            route.getSchemaHash(),
                            route.getEndpoints())));
        }
        return ImmutableMap.copyOf(tablets);
    }

    private Map<String, String> buildExecutionProperties(StarRocksConnectorConfig cfg) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (!cfg.getFeHttpUrls().isEmpty()) {
            builder.put("fe_http_urls", String.join(";", cfg.getFeHttpUrls()));
        }
        builder.put("fe_jdbc_url", cfg.getFeJdbcUrl());
        builder.put("user", cfg.getUser());
        if (cfg.getPassword() != null) {
            builder.put("password", cfg.getPassword());
        }
        if (!cfg.getBeRpcEndpoints().isEmpty()) {
            builder.put("be_rpc_endpoints", String.join(";", cfg.getBeRpcEndpoints()));
        }
        builder.put("fetch_mode", cfg.getFetchMode());
        builder.put("request_retries", Integer.toString(cfg.getRequestRetries()));
        builder.put("connect_timeout_ms", Integer.toString(cfg.getConnectTimeoutMs()));
        builder.put("read_timeout_ms", Integer.toString(cfg.getReadTimeoutMs()));
        return builder.build();
    }

    @Override
    public void close() {
        try {
            jdbcClient.close();
        } catch (Exception e) {
            LOG.warn("Failed to close JDBC client for starrocks catalog {}: {}", context.getCatalogName(), e.getMessage());
        }
        try {
            restClient.close();
        } catch (Exception e) {
            LOG.warn("Failed to close REST client for starrocks catalog {}: {}", context.getCatalogName(), e.getMessage());
        }
    }

    private static final class CacheEntry<T> {
        private final T value;
        private final long expireAt;

        private CacheEntry(T value, long expireAt) {
            this.value = value;
            this.expireAt = expireAt;
        }

        private boolean isValid(long now) {
            return now <= expireAt;
        }
    }

    private static final class TableCacheKey {
        private final String dbName;
        private final String tableName;
        private final String normalizedDb;
        private final String normalizedTable;

        private TableCacheKey(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.normalizedDb = dbName.toLowerCase(Locale.ROOT);
            this.normalizedTable = tableName.toLowerCase(Locale.ROOT);
        }

        static TableCacheKey of(String dbName, String tableName) {
            return new TableCacheKey(dbName, tableName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableCacheKey)) {
                return false;
            }
            TableCacheKey other = (TableCacheKey) o;
            return normalizedDb.equals(other.normalizedDb)
                    && normalizedTable.equals(other.normalizedTable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(normalizedDb, normalizedTable);
        }
    }
}

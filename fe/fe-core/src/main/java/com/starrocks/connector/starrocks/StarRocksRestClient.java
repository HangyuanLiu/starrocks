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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.http.rest.v2.RestBaseResultV2;
import com.starrocks.http.rest.v2.RestBaseResultV2.PagedResult;
import com.starrocks.http.rest.v2.vo.ColumnView;
import com.starrocks.http.rest.v2.vo.PartitionInfoView;
import com.starrocks.http.rest.v2.vo.TableSchemaView;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * REST client talking to remote StarRocks FE `_query_plan` endpoint.
 */
public interface StarRocksRestClient extends Closeable {

    Logger LOG = LogManager.getLogger(StarRocksRestClient.class);
    MediaType JSON = MediaType.get("application/json; charset=utf-8");

    static StarRocksRestClient create(StarRocksConnectorConfig config) {
        return new DefaultStarRocksRestClient(config);
    }

    QueryPlanResponse getQueryPlan(String dbName, String tableName, String sql);

    /**
     * Fetch partition metadata from Provider FE v2 API.
     * Returns paginated list of PartitionView including storagePath and tablets.
     */
    PartitionMetadataResponse getPartitionMetadata(String catalogName, String dbName, String tableName);

    TableSchemaView getTableSchema(String catalogName, String dbName, String tableName);

    List<PartitionInfoView.PartitionView> listTablePartitions(String catalogName, String dbName, String tableName);

    TransactionResult beginTransaction(String catalogName, String dbName, String tableName, String label, int timeoutSecs);

    TransactionResult prepareTransaction(String catalogName, String dbName, String label,
                                         List<TTabletCommitInfo> successTablets, List<TTabletFailInfo> failureTablets);

    TransactionResult commitTransaction(String catalogName, String dbName, String label);

    TransactionResult rollbackTransaction(String catalogName, String dbName, String label,
                                          List<TTabletFailInfo> failureTablets);

    @Override
    void close();

    final class DefaultStarRocksRestClient implements StarRocksRestClient {

        private static final int DEFAULT_PAGE_SIZE = 100;
        private static final int BYPASS_WRITE_JOB_SOURCE_TYPE = 11;
        private static final String HEADER_DATABASE = "db";
        private static final String HEADER_TABLE = "table";
        private static final String HEADER_LABEL = "label";
        private static final String HEADER_TIMEOUT = "timeout";
        private static final String PARAM_PAGE_NUM = "page_num";
        private static final String PARAM_PAGE_SIZE = "page_size";
        private static final String PARAM_TEMPORARY = "temporary";
        private static final String PARAM_SOURCE_TYPE = "source_type";
        private static final String BODY_COMMITTED_TABLETS = "committed_tablets";
        private static final String BODY_FAILED_TABLETS = "failed_tablets";

        private static final Gson GSON = new GsonBuilder()
                .registerTypeAdapter(ColumnView.TypeView.class, new ColumnTypeViewDeserializer())
                .create();

        private static final class ColumnTypeViewDeserializer implements JsonDeserializer<ColumnView.TypeView> {
            @Override
            public ColumnView.TypeView deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                    throws JsonParseException {
                if (json == null || json.isJsonNull()) {
                    return null;
                }
                if (!json.isJsonObject()) {
                    throw new JsonParseException("Invalid column type view: " + json);
                }
                JsonObject obj = json.getAsJsonObject();
                if (obj.has("itemType")) {
                    return context.deserialize(json, ColumnView.ArrayTypeView.class);
                }
                if (obj.has("fields")) {
                    return context.deserialize(json, ColumnView.StructTypeView.class);
                }
                if (obj.has("keyType") || obj.has("valueType")) {
                    return context.deserialize(json, ColumnView.MapTypeView.class);
                }
                return context.deserialize(json, ColumnView.ScalarTypeView.class);
            }
        }

        private final List<String> endpoints;
        private final OkHttpClient httpClient;
        private final String authorizationHeader;
        private final int retries;

        DefaultStarRocksRestClient(StarRocksConnectorConfig config) {
            Preconditions.checkNotNull(config, "config is null");
            this.endpoints = normalizeEndpoints(config.getFeHttpUrls());
            if (endpoints.isEmpty()) {
                throw new StarRocksConnectorException("No FE HTTP endpoints configured for starrocks catalog");
            }
            this.authorizationHeader = Credentials.basic(
                    Objects.requireNonNull(config.getUser(), "starrocks.user is null"),
                    Objects.requireNonNullElse(config.getPassword(), "")
            );
            this.retries = Math.max(1, config.getRequestRetries());
            this.httpClient = new OkHttpClient.Builder()
                    .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                    .readTimeout(config.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                    .retryOnConnectionFailure(true)
                    .build();
        }

        private static List<String> normalizeEndpoints(List<String> rawEndpoints) {
            if (rawEndpoints == null || rawEndpoints.isEmpty()) {
                return List.of();
            }
            ImmutableList.Builder<String> builder = ImmutableList.builderWithExpectedSize(rawEndpoints.size());
            for (String endpoint : rawEndpoints) {
                if (endpoint == null || endpoint.isEmpty()) {
                    continue;
                }
                String trimmed = endpoint.trim();
                if (trimmed.endsWith("/")) {
                    trimmed = trimmed.substring(0, trimmed.length() - 1);
                }
                builder.add(trimmed);
            }
            return builder.build();
        }

        @Override
        public QueryPlanResponse getQueryPlan(String dbName, String tableName, String sql) {
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(tableName, "tableName is null");
            Preconditions.checkNotNull(sql, "sql is null");

            StarRocksConnectorException lastError = null;
            int attempt = 0;
            int maxAttempts = Math.max(1, retries) * endpoints.size();
            while (attempt < maxAttempts) {
                String endpoint = endpoints.get(attempt % endpoints.size());
                attempt++;
                try {
                    HttpUrl url = buildUrl(endpoint, dbName, tableName);
                    JsonObject payload = new JsonObject();
                    payload.addProperty("sql", sql);
                    RequestBody body = RequestBody.create(payload.toString(), JSON);
                    Request request = new Request.Builder()
                            .url(url)
                            .post(body)
                            .addHeader("Authorization", authorizationHeader)
                            .addHeader("Content-Type", "application/json")
                            .build();
                    try (Response response = httpClient.newCall(request).execute()) {
                        if (!response.isSuccessful()) {
                            String responseBody = response.body() != null ? response.body().string() : "";
                            String message = String.format(Locale.ROOT,
                                    "HTTP %d when fetching query plan from %s: %s",
                                    response.code(), url, responseBody);
                            lastError = new StarRocksConnectorException(message);
                            LOG.warn(message);
                            continue;
                        }
                        String bodyString = response.body() != null ? response.body().string() : "";
                        return parseResponse(bodyString, url.toString());
                    }
                } catch (IOException e) {
                    lastError = new StarRocksConnectorException(
                            String.format(Locale.ROOT,
                                    "Failed to call _query_plan on endpoint %s for %s.%s",
                                    endpoint, dbName, tableName), e);
                    LOG.warn("Attempt to fetch query plan from {} failed: {}", endpoint, e.getMessage());
                }
            }
            if (lastError != null) {
                throw lastError;
            }
            throw new StarRocksConnectorException("Unknown error fetching query plan");
        }

        @Override
        public TableSchemaView getTableSchema(String catalogName, String dbName, String tableName) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(tableName, "tableName is null");

            StarRocksConnectorException lastError = null;
            int attempt = 0;
            int maxAttempts = Math.max(1, retries) * endpoints.size();
            while (attempt < maxAttempts) {
                String endpoint = endpoints.get(attempt % endpoints.size());
                attempt++;
                try {
                    HttpUrl url = buildSchemaUrl(endpoint, catalogName, dbName, tableName);
                    Request request = new Request.Builder()
                            .url(url)
                            .get()
                            .addHeader("Authorization", authorizationHeader)
                            .build();
                    try (Response response = httpClient.newCall(request).execute()) {
                        String bodyString = response.body() != null ? response.body().string() : "";
                        if (!response.isSuccessful()) {
                            String message = String.format(Locale.ROOT,
                                    "HTTP %d when fetching schema from %s: %s",
                                    response.code(), url, bodyString);
                            lastError = new StarRocksConnectorException(message);
                            LOG.warn(message);
                            continue;
                        }
                        return parseSchemaResponse(bodyString, url.toString());
                    }
                } catch (IOException e) {
                    lastError = new StarRocksConnectorException(
                            String.format(Locale.ROOT,
                                    "Failed to call schema API on endpoint %s for %s.%s.%s",
                                    endpoint, catalogName, dbName, tableName), e);
                    LOG.warn("Attempt to fetch schema from {} failed: {}", endpoint, e.getMessage());
                }
            }
            if (lastError != null) {
                throw lastError;
            }
            throw new StarRocksConnectorException("Unknown error fetching schema");
        }

        @Override
        public List<PartitionInfoView.PartitionView> listTablePartitions(String catalogName, String dbName, String tableName) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(tableName, "tableName is null");

            List<PartitionInfoView.PartitionView> partitions = new ArrayList<>();
            int pageNum = 0;
            while (true) {
                PagedResult<PartitionInfoView.PartitionView> page =
                        fetchPartitionPage(catalogName, dbName, tableName, pageNum, DEFAULT_PAGE_SIZE);
                if (page == null || page.getItems() == null) {
                    break;
                }
                partitions.addAll(page.getItems());
                Integer totalPages = page.getPages();
                if (totalPages == null || totalPages <= 0) {
                    break;
                }
                pageNum++;
                if (pageNum >= totalPages) {
                    break;
                }
            }
            return partitions;
        }

        @Override
        public TransactionResult beginTransaction(String catalogName, String dbName, String tableName, String label,
                                                  int timeoutSecs) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(tableName, "tableName is null");
            Preconditions.checkNotNull(label, "label is null");

            RequestBody body = RequestBody.create("", JSON);
            return doTransaction("begin", dbName, tableName, label, timeoutSecs, body, BYPASS_WRITE_JOB_SOURCE_TYPE);
        }

        @Override
        public TransactionResult prepareTransaction(String catalogName, String dbName, String label,
                                                    List<TTabletCommitInfo> successTablets,
                                                    List<TTabletFailInfo> failureTablets) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(label, "label is null");

            Map<String, Object> payload = new LinkedHashMap<>();
            if (successTablets != null && !successTablets.isEmpty()) {
                payload.put(BODY_COMMITTED_TABLETS, toCommitTabletPayload(successTablets));
            }
            if (failureTablets != null && !failureTablets.isEmpty()) {
                payload.put(BODY_FAILED_TABLETS, toFailTabletPayload(failureTablets));
            }
            RequestBody body = RequestBody.create(GSON.toJson(payload), JSON);
            return doTransaction("prepare", dbName, null, label, null, body, null);
        }

        @Override
        public TransactionResult commitTransaction(String catalogName, String dbName, String label) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(label, "label is null");

            RequestBody body = RequestBody.create("", JSON);
            return doTransaction("commit", dbName, null, label, null, body, null);
        }

        @Override
        public TransactionResult rollbackTransaction(String catalogName, String dbName, String label,
                                                     List<TTabletFailInfo> failureTablets) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(label, "label is null");

            Map<String, Object> payload = new LinkedHashMap<>();
            if (failureTablets != null && !failureTablets.isEmpty()) {
                payload.put(BODY_FAILED_TABLETS, toFailTabletPayload(failureTablets));
            }
            RequestBody body = RequestBody.create(GSON.toJson(payload), JSON);
            return doTransaction("rollback", dbName, null, label, null, body, null);
        }

        private static List<Map<String, Object>> toCommitTabletPayload(List<TTabletCommitInfo> tabletCommitInfos) {
            List<Map<String, Object>> payload = new ArrayList<>();
            for (TTabletCommitInfo info : tabletCommitInfos) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("tabletId", info.getTabletId());
                item.put("backendId", info.getBackendId());
                if (info.isSetInvalid_dict_cache_columns()) {
                    item.put("invalidDictCacheColumns", info.getInvalid_dict_cache_columns());
                    item.put("validDictCacheColumns", info.getValid_dict_cache_columns());
                    item.put("validDictCollectedVersions", info.getValid_dict_collected_versions());
                }
                payload.add(item);
            }
            return payload;
        }

        private static List<Map<String, Object>> toFailTabletPayload(List<TTabletFailInfo> tabletFailInfos) {
            List<Map<String, Object>> payload = new ArrayList<>();
            for (TTabletFailInfo info : tabletFailInfos) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("tabletId", info.getTabletId());
                item.put("backendId", info.getBackendId());
                payload.add(item);
            }
            return payload;
        }

        private static HttpUrl buildUrl(String endpoint, String dbName, String tableName) {
            HttpUrl base = HttpUrl.parse(endpoint);
            if (base == null) {
                throw new StarRocksConnectorException("Invalid FE http endpoint: " + endpoint);
            }
            return base.newBuilder()
                    .addPathSegment("api")
                    .addPathSegment(dbName)
                    .addPathSegment(tableName)
                    .addPathSegment("_query_plan")
                    .build();
        }

        private static HttpUrl buildSchemaUrl(String endpoint, String catalogName, String dbName, String tableName) {
            HttpUrl base = HttpUrl.parse(endpoint);
            if (base == null) {
                throw new StarRocksConnectorException("Invalid FE http endpoint: " + endpoint);
            }
            return base.newBuilder()
                    .addPathSegment("api")
                    .addPathSegment("v2")
                    .addPathSegment("catalogs")
                    .addPathSegment(catalogName)
                    .addPathSegment("databases")
                    .addPathSegment(dbName)
                    .addPathSegment("tables")
                    .addPathSegment(tableName)
                    .addPathSegment("schema")
                    .build();
        }

        private static QueryPlanResponse parseResponse(String body, String url) {
            try {
                JsonObject root = JsonParser.parseString(body).getAsJsonObject();
                int status = root.get("status").getAsInt();
                if (status != 200) {
                    String message = null;
                    if (root.has("exception")) {
                        message = root.get("exception").getAsString();
                    } else if (root.has("msg")) {
                        message = root.get("msg").getAsString();
                    }
                    if (message == null) {
                        message = body;
                    }
                    throw new StarRocksConnectorException(
                            String.format(Locale.ROOT, "FE %s returned status %d: %s", url, status, message));
                }
                String encodedPlan = root.has("opaqued_query_plan") ? root.get("opaqued_query_plan").getAsString() : "";
                Map<Long, QueryPlanResponse.TabletRouting> tablets = parseTablets(root);
                return new QueryPlanResponse(encodedPlan, tablets);
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to parse _query_plan response: " + body, e);
            }
        }

        private static Map<Long, QueryPlanResponse.TabletRouting> parseTablets(JsonObject root) {
            if (!root.has("partitions") || !root.get("partitions").isJsonObject()) {
                return ImmutableMap.of();
            }
            JsonObject partitions = root.getAsJsonObject("partitions");
            Map<Long, QueryPlanResponse.TabletRouting> result = new LinkedHashMap<>();
            for (Map.Entry<String, JsonElement> entry : partitions.entrySet()) {
                String tabletIdStr = entry.getKey();
                if (!entry.getValue().isJsonObject()) {
                    continue;
                }
                JsonObject tabletJson = entry.getValue().getAsJsonObject();
                long tabletId;
                try {
                    tabletId = Long.parseLong(tabletIdStr);
                } catch (NumberFormatException ex) {
                    LOG.warn("Invalid tablet id '{}' in _query_plan response", tabletIdStr);
                    continue;
                }
                List<String> endpoints = new ArrayList<>();
                if (tabletJson.has("routings") && tabletJson.get("routings").isJsonArray()) {
                    tabletJson.getAsJsonArray("routings").forEach(elem -> endpoints.add(elem.getAsString()));
                }
                long version = tabletJson.has("version") ? tabletJson.get("version").getAsLong() : -1L;
                int schemaHash = tabletJson.has("schemaHash") ? tabletJson.get("schemaHash").getAsInt() : 0;
                result.put(tabletId, new QueryPlanResponse.TabletRouting(endpoints, version, schemaHash));
            }
            return ImmutableMap.copyOf(result);
        }

        private static TableSchemaView parseSchemaResponse(String body, String url) {
            try {
                Type type = new TypeToken<RestBaseResultV2<TableSchemaView>>() {
                }.getType();
                RestBaseResultV2<TableSchemaView> response = GSON.fromJson(body, type);
                if (response == null) {
                    throw new StarRocksConnectorException("Empty schema response: " + body);
                }
                if (response.getCode() != null && !"0".equals(response.getCode())) {
                    String message = response.getMessage() != null ? response.getMessage() : body;
                    throw new StarRocksConnectorException(
                            String.format(Locale.ROOT, "FE %s returned code %s: %s", url, response.getCode(), message));
                }
                return response.getResult();
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to parse schema response: " + body, e);
            }
        }

        @Override
        public void close() {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }

        @Override
        public PartitionMetadataResponse getPartitionMetadata(String catalogName, String dbName, String tableName) {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            Preconditions.checkNotNull(dbName, "dbName is null");
            Preconditions.checkNotNull(tableName, "tableName is null");

            Map<Long, String> tabletRoots = new LinkedHashMap<>();
            int pageNum = 0;
            int pageSize = 100;

            StarRocksConnectorException lastError = null;
            while (true) {
                for (int attempt = 0; attempt < retries; attempt++) {
                    String endpoint = endpoints.get(attempt % endpoints.size());
                    try {
                        HttpUrl url = buildPartitionUrl(endpoint, catalogName, dbName, tableName, pageNum, pageSize, false);
                        Request request = new Request.Builder()
                                .url(url)
                                .get()
                                .addHeader("Authorization", authorizationHeader)
                                .build();

                        try (Response response = httpClient.newCall(request).execute()) {
                            if (!response.isSuccessful()) {
                                String responseBody = response.body() != null ? response.body().string() : "";
                                lastError = new StarRocksConnectorException(
                                        String.format(Locale.ROOT,
                                                "HTTP %d when fetching partition metadata from %s: %s",
                                                response.code(), url, responseBody));
                                LOG.warn(lastError.getMessage());
                                continue;
                            }

                            String bodyString = response.body() != null ? response.body().string() : "";
                            LOG.info("Partition API response for {}.{}.{}: {}", 
                                    catalogName, dbName, tableName, bodyString);
                            PartitionPage page = parsePartitionResponse(bodyString, url.toString());
                            
                            // Collect tablet -> storagePath mapping
                            for (PartitionEntry partition : page.partitions) {
                                if (partition.storagePath != null && partition.tablets != null) {
                                    LOG.info("Partition storagePath: {}, tablets: {}", 
                                            partition.storagePath, partition.tablets.size());
                                    for (TabletEntry tablet : partition.tablets) {
                                        tabletRoots.put(tablet.id, partition.storagePath);
                                        LOG.info("Mapped tablet {} to storagePath: {}", 
                                                tablet.id, partition.storagePath);
                                    }
                                }
                            }

                            // Check if more pages exist
                            if (pageNum >= page.pages - 1) {
                                return new PartitionMetadataResponse(tabletRoots);
                            }
                            pageNum++;
                            break; // Success, proceed to next page
                        }
                    } catch (IOException e) {
                        lastError = new StarRocksConnectorException(
                                String.format(Locale.ROOT,
                                        "Failed to call partition API on endpoint %s for %s.%s.%s",
                                        endpoint, catalogName, dbName, tableName), e);
                        LOG.warn("Attempt to fetch partition metadata from {} failed: {}", endpoint, e.getMessage());
                    }
                }

                if (lastError != null) {
                    throw lastError;
                }
            }
        }

        private static HttpUrl buildPartitionUrl(String endpoint, String catalogName, String dbName,
                                                 String tableName, int pageNum, int pageSize, boolean temporary) {
            HttpUrl base = HttpUrl.parse(endpoint);
            if (base == null) {
                throw new StarRocksConnectorException("Invalid FE http endpoint: " + endpoint);
            }
            return base.newBuilder()
                    .addPathSegment("api")
                    .addPathSegment("v2")
                    .addPathSegment("catalogs")
                    .addPathSegment(catalogName)
                    .addPathSegment("databases")
                    .addPathSegment(dbName)
                    .addPathSegment("tables")
                    .addPathSegment(tableName)
                    .addPathSegment("partition")
                    .addQueryParameter(PARAM_PAGE_NUM, String.valueOf(pageNum))
                    .addQueryParameter(PARAM_PAGE_SIZE, String.valueOf(pageSize))
                    .addQueryParameter(PARAM_TEMPORARY, String.valueOf(temporary))
                    .build();
        }

        private PagedResult<PartitionInfoView.PartitionView> fetchPartitionPage(String catalogName, String dbName,
                                                                                String tableName, int pageNum,
                                                                                int pageSize) {
            StarRocksConnectorException lastError = null;
            int attempt = 0;
            int maxAttempts = Math.max(1, retries) * endpoints.size();
            while (attempt < maxAttempts) {
                String endpoint = endpoints.get(attempt % endpoints.size());
                attempt++;
                try {
                    HttpUrl url = buildPartitionUrl(endpoint, catalogName, dbName, tableName, pageNum, pageSize, false);
                    Request request = new Request.Builder()
                            .url(url)
                            .get()
                            .addHeader("Authorization", authorizationHeader)
                            .build();
                    try (Response response = httpClient.newCall(request).execute()) {
                        String bodyString = response.body() != null ? response.body().string() : "";
                        if (!response.isSuccessful()) {
                            lastError = new StarRocksConnectorException(
                                    String.format(Locale.ROOT,
                                            "HTTP %d when fetching partitions from %s: %s",
                                            response.code(), url, bodyString));
                            LOG.warn(lastError.getMessage());
                            continue;
                        }
                        return parsePartitionPage(bodyString, url.toString());
                    }
                } catch (IOException e) {
                    lastError = new StarRocksConnectorException(
                            String.format(Locale.ROOT,
                                    "Failed to call partition API on endpoint %s for %s.%s.%s",
                                    endpoint, catalogName, dbName, tableName), e);
                    LOG.warn("Attempt to fetch partitions from {} failed: {}", endpoint, e.getMessage());
                }
            }
            if (lastError != null) {
                throw lastError;
            }
            throw new StarRocksConnectorException("Unknown error fetching partitions");
        }

        private static PagedResult<PartitionInfoView.PartitionView> parsePartitionPage(String body, String url) {
            try {
                Type type = new TypeToken<RestBaseResultV2<PagedResult<PartitionInfoView.PartitionView>>>() {
                }.getType();
                RestBaseResultV2<PagedResult<PartitionInfoView.PartitionView>> response = GSON.fromJson(body, type);
                if (response == null) {
                    throw new StarRocksConnectorException("Empty partition response: " + body);
                }
                if (response.getCode() != null && !"0".equals(response.getCode())) {
                    String message = response.getMessage() != null ? response.getMessage() : body;
                    throw new StarRocksConnectorException(
                            String.format(Locale.ROOT, "FE %s returned code %s: %s", url, response.getCode(), message));
                }
                return response.getResult();
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to parse partition response: " + body, e);
            }
        }

        private static HttpUrl buildTransactionUrl(String endpoint, String operation, Integer sourceType) {
            HttpUrl base = HttpUrl.parse(endpoint);
            if (base == null) {
                throw new StarRocksConnectorException("Invalid FE http endpoint: " + endpoint);
            }
            HttpUrl.Builder builder = base.newBuilder()
                    .addPathSegment("api")
                    .addPathSegment("transaction")
                    .addPathSegment(operation);
            if (sourceType != null) {
                builder.addQueryParameter(PARAM_SOURCE_TYPE, String.valueOf(sourceType));
            }
            return builder.build();
        }

        private TransactionResult doTransaction(String operation, String dbName, String tableName, String label,
                                                Integer timeoutSecs, RequestBody body, Integer sourceType) {
            StarRocksConnectorException lastError = null;
            int attempt = 0;
            int maxAttempts = Math.max(1, retries) * endpoints.size();
            while (attempt < maxAttempts) {
                String endpoint = endpoints.get(attempt % endpoints.size());
                attempt++;
                try {
                    HttpUrl url = buildTransactionUrl(endpoint, operation, sourceType);
                    Request.Builder builder = new Request.Builder()
                            .url(url)
                            .post(body)
                            .addHeader("Authorization", authorizationHeader)
                            .addHeader("Content-Type", "application/json")
                            .addHeader(HEADER_DATABASE, dbName)
                            .addHeader(HEADER_LABEL, label);
                    if (tableName != null) {
                        builder.addHeader(HEADER_TABLE, tableName);
                    }
                    if (timeoutSecs != null && timeoutSecs > 0) {
                        builder.addHeader(HEADER_TIMEOUT, String.valueOf(timeoutSecs));
                    }
                    Request request = builder.build();
                    try (Response response = httpClient.newCall(request).execute()) {
                        String bodyString = response.body() != null ? response.body().string() : "";
                        if (!response.isSuccessful()) {
                            String message = String.format(Locale.ROOT,
                                    "HTTP %d when executing transaction %s on %s: %s",
                                    response.code(), operation, url, bodyString);
                            lastError = new StarRocksConnectorException(message);
                            LOG.warn(message);
                            continue;
                        }
                        TransactionResult result = parseTransactionResult(bodyString, url.toString());
                        if (result.isOk()) {
                            return result;
                        }
                        String message = String.format(Locale.ROOT,
                                "Transaction %s failed on %s: %s", operation, url, result.getMessage());
                        lastError = new StarRocksConnectorException(message);
                        LOG.warn(message);
                    }
                } catch (IOException e) {
                    lastError = new StarRocksConnectorException(
                            String.format(Locale.ROOT, "Failed to call transaction %s on endpoint %s",
                                    operation, endpoint), e);
                    LOG.warn("Attempt to execute transaction {} on {} failed: {}", operation, endpoint, e.getMessage());
                }
            }
            if (lastError != null) {
                throw lastError;
            }
            throw new StarRocksConnectorException("Unknown error executing transaction " + operation);
        }

        private static TransactionResult parseTransactionResult(String body, String url) {
            try {
                TransactionResult result = GSON.fromJson(body, TransactionResult.class);
                if (result == null) {
                    throw new StarRocksConnectorException("Empty transaction response: " + body);
                }
                return result;
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to parse transaction response: " + body, e);
            }
        }

        private static PartitionPage parsePartitionResponse(String body, String url) {
            try {
                JsonObject root = JsonParser.parseString(body).getAsJsonObject();
                
                // Check code field (v2 API uses "code" instead of "status")
                if (root.has("code")) {
                    String code = root.get("code").getAsString();
                    if (!"0".equals(code)) {
                        String message = root.has("message") ? root.get("message").getAsString() : body;
                        throw new StarRocksConnectorException(
                                String.format(Locale.ROOT, "FE %s returned code %s: %s", url, code, message));
                    }
                }

                if (!root.has("result") || !root.get("result").isJsonObject()) {
                    throw new StarRocksConnectorException("Missing 'result' in partition API response");
                }

                JsonObject result = root.getAsJsonObject("result");
                int pages = result.has("pages") ? result.get("pages").getAsInt() : 1;
                
                List<PartitionEntry> partitions = new ArrayList<>();
                if (result.has("items") && result.get("items").isJsonArray()) {
                    for (JsonElement item : result.getAsJsonArray("items")) {
                        if (!item.isJsonObject()) {
                            continue;
                        }
                        JsonObject partitionJson = item.getAsJsonObject();
                        String storagePath = partitionJson.has("storagePath") 
                                ? partitionJson.get("storagePath").getAsString() : null;
                        
                        List<TabletEntry> tablets = new ArrayList<>();
                        if (partitionJson.has("tablets") && partitionJson.get("tablets").isJsonArray()) {
                            for (JsonElement tabletElem : partitionJson.getAsJsonArray("tablets")) {
                                if (!tabletElem.isJsonObject()) {
                                    continue;
                                }
                                JsonObject tabletJson = tabletElem.getAsJsonObject();
                                if (tabletJson.has("id")) {
                                    tablets.add(new TabletEntry(tabletJson.get("id").getAsLong()));
                                }
                            }
                        }
                        
                        partitions.add(new PartitionEntry(storagePath, tablets));
                    }
                }

                return new PartitionPage(pages, partitions);
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to parse partition API response: " + body, e);
            }
        }

        private static class PartitionPage {
            final int pages;
            final List<PartitionEntry> partitions;

            PartitionPage(int pages, List<PartitionEntry> partitions) {
                this.pages = pages;
                this.partitions = partitions;
            }
        }

        private static class PartitionEntry {
            final String storagePath;
            final List<TabletEntry> tablets;

            PartitionEntry(String storagePath, List<TabletEntry> tablets) {
                this.storagePath = storagePath;
                this.tablets = tablets;
            }
        }

        private static class TabletEntry {
            final long id;

            TabletEntry(long id) {
                this.id = id;
            }
        }
    }

    final class QueryPlanResponse {
        private final String opaquedQueryPlan;
        private final Map<Long, TabletRouting> tablets;

        QueryPlanResponse(String opaquedQueryPlan, Map<Long, TabletRouting> tablets) {
            this.opaquedQueryPlan = opaquedQueryPlan;
            this.tablets = tablets;
        }

        public String getOpaquedQueryPlan() {
            return opaquedQueryPlan;
        }

        public Map<Long, TabletRouting> getTablets() {
            return tablets;
        }

        public static class TabletRouting {
            private final List<String> endpoints;
            private final long version;
            private final int schemaHash;

            public TabletRouting(List<String> endpoints, long version, int schemaHash) {
                this.endpoints = ImmutableList.copyOf(endpoints);
                this.version = version;
                this.schemaHash = schemaHash;
            }

            public List<String> getEndpoints() {
                return endpoints;
            }

            public long getVersion() {
                return version;
            }

            public int getSchemaHash() {
                return schemaHash;
            }
        }
    }

    /**
     * Partition metadata response from v2 partition API.
     */
    final class PartitionMetadataResponse {
        private final Map<Long, String> tabletStoragePaths;

        public PartitionMetadataResponse(Map<Long, String> tabletStoragePaths) {
            this.tabletStoragePaths = ImmutableMap.copyOf(tabletStoragePaths);
        }

        public Map<Long, String> getTabletStoragePaths() {
            return tabletStoragePaths;
        }
    }

    final class TransactionResult {
        @SerializedName("Status")
        private String status;

        @SerializedName("Message")
        private String message;

        @SerializedName("Label")
        private String label;

        @SerializedName("TxnId")
        private Long txnId;

        public boolean isOk() {
            return status != null && "OK".equalsIgnoreCase(status);
        }

        public String getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public String getLabel() {
            return label;
        }

        public Long getTxnId() {
            return txnId;
        }
    }
}

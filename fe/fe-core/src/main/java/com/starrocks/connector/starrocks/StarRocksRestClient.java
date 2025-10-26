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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.connector.exception.StarRocksConnectorException;
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

    @Override
    void close();

    final class DefaultStarRocksRestClient implements StarRocksRestClient {

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

        @Override
        public void close() {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
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
}

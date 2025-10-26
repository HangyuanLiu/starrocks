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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.starrocks.connector.config.Config;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Configuration holder for the StarRocks external catalog in RPC mode.
 * <p>
 * The config keys mirror the Spark/Trino connector options so that future integration can reuse the same semantics.
 */
public class StarRocksConnectorConfig extends ConnectorConfig {

    public static final String KEY_FETCH_MODE = "starrocks.fetch.mode";
    public static final String KEY_FE_HTTP_URL = "starrocks.fe.http.url";
    public static final String KEY_FE_JDBC_URL = "starrocks.fe.jdbc.url";
    public static final String KEY_USER = "starrocks.user";
    public static final String KEY_PASSWORD = "starrocks.password";
    public static final String KEY_BE_RPC_ENDPOINTS = "starrocks.be.rpc.endpoints";
    public static final String KEY_REQUEST_RETRIES = "starrocks.request.retries";
    public static final String KEY_CONNECT_TIMEOUT = "starrocks.request.connect.timeout.ms";
    public static final String KEY_READ_TIMEOUT = "starrocks.request.read.timeout.ms";

    private static final Splitter ENDPOINT_SPLITTER = Splitter.on(';').trimResults().omitEmptyStrings();

    @Config(key = KEY_FE_HTTP_URL, desc = "StarRocks FE HTTP endpoints used for _query_plan", nullable = false, defaultValue = "")
    private String feHttpUrls;

    @Config(key = KEY_FE_JDBC_URL, desc = "StarRocks FE MySQL endpoint for metadata access", nullable = false, defaultValue = "")
    private String feJdbcUrl;

    @Config(key = KEY_USER, desc = "Username for accessing the external StarRocks cluster", nullable = false, defaultValue = "")
    private String user;

    @Config(key = KEY_PASSWORD, desc = "Password for accessing the external StarRocks cluster", nullable = true,
            defaultValue = "")
    private String password;

    @Config(key = KEY_BE_RPC_ENDPOINTS,
            desc = "StarRocks BE/CN RPC endpoints for scanner RPC, semicolon separated host:port list",
            nullable = true, defaultValue = "")
    private String beRpcEndpoints;

    @Config(key = KEY_FETCH_MODE,
            desc = "Fetch mode for accessing external StarRocks data, rpc or object_store",
            defaultValue = "rpc")
    private String fetchMode = "rpc";

    @Config(key = KEY_REQUEST_RETRIES,
            desc = "Retry count for REST/RPC requests",
            defaultValue = "3")
    private String requestRetries = "3";

    @Config(key = KEY_CONNECT_TIMEOUT,
            desc = "Connection timeout in milliseconds for REST/RPC requests",
            defaultValue = "10000")
    private String connectTimeoutMs = "10000";

    @Config(key = KEY_READ_TIMEOUT,
            desc = "Read timeout in milliseconds for REST/RPC requests",
            defaultValue = "30000")
    private String readTimeoutMs = "30000";

    public List<String> getFeHttpUrls() {
        if (Strings.isNullOrEmpty(feHttpUrls)) {
            return Collections.emptyList();
        }
        return ENDPOINT_SPLITTER.splitToList(feHttpUrls);
    }

    public String getFeJdbcUrl() {
        return feJdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public List<String> getBeRpcEndpoints() {
        if (Strings.isNullOrEmpty(beRpcEndpoints)) {
            return Collections.emptyList();
        }
        return ENDPOINT_SPLITTER.splitToList(beRpcEndpoints);
    }

    public String getFetchMode() {
        return fetchMode.toLowerCase(Locale.ROOT);
    }

    public int getRequestRetries() {
        return Integer.parseInt(requestRetries);
    }

    public int getConnectTimeoutMs() {
        return Integer.parseInt(connectTimeoutMs);
    }

    public int getReadTimeoutMs() {
        return Integer.parseInt(readTimeoutMs);
    }

    @Override
    public void loadConfig(java.util.Map<String, String> properties) {
        super.loadConfig(properties);
        validate();
    }

    private void validate() {
        if (Strings.isNullOrEmpty(feHttpUrls)) {
            throw new StarRocksConnectorException("Property '" + KEY_FE_HTTP_URL + "' is required for starrocks catalog");
        }
        if (Strings.isNullOrEmpty(feJdbcUrl)) {
            throw new StarRocksConnectorException("Property '" + KEY_FE_JDBC_URL + "' is required for starrocks catalog");
        }
        if (Strings.isNullOrEmpty(user)) {
            throw new StarRocksConnectorException("Property '" + KEY_USER + "' is required for starrocks catalog");
        }
        String fetchModeLower = getFetchMode();
        if (!"rpc".equals(fetchModeLower) && !"object_store".equals(fetchModeLower)) {
            throw new StarRocksConnectorException(
                    "Unsupported fetch mode '" + fetchMode + "', valid values are 'rpc' or 'object_store'");
        }
        if ("rpc".equals(fetchModeLower) && Strings.isNullOrEmpty(beRpcEndpoints)) {
            throw new StarRocksConnectorException(
                    "Property '" + KEY_BE_RPC_ENDPOINTS + "' is required when fetch.mode = rpc");
        }
    }
}

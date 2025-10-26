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

import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Metadata implementation for the StarRocks external catalog leveraging JDBC metadata and REST `_query_plan`.
 */
public class StarRocksConnectorMetadata implements ConnectorMetadata, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(StarRocksConnectorMetadata.class);

    @FunctionalInterface
    interface CacheFactory {
        StarRocksMetadataCache create(ConnectorContext context, StarRocksConnectorConfig config);
    }

    private final ConnectorContext context;
    private final CacheFactory cacheFactory;
    private StarRocksConnectorConfig config;
    private StarRocksMetadataCache metadataCache;

    public StarRocksConnectorMetadata(ConnectorContext context) {
        this(context, StarRocksMetadataCache::new);
    }

    StarRocksConnectorMetadata(ConnectorContext context, CacheFactory cacheFactory) {
        this.context = context;
        this.cacheFactory = cacheFactory;
    }

    public void bindConfig(StarRocksConnectorConfig config) {
        if (metadataCache != null) {
            metadataCache.close();
        }
        this.config = config;
        this.metadataCache = cacheFactory.create(context, config);
    }

    private StarRocksMetadataCache requireCache() {
        if (metadataCache == null) {
            throw new StarRocksConnectorException(
                    "StarRocks catalog '" + context.getCatalogName() + "' has not been initialized");
        }
        return metadataCache;
    }

    @Override
    public List<String> listDbNames(ConnectContext connectContext) {
        return requireCache().listDatabases();
    }

    @Override
    public List<String> listTableNames(ConnectContext connectContext, String dbName) {
        return requireCache().listTables(dbName);
    }

    @Override
    public Table getTable(ConnectContext connectContext, String dbName, String tableName) {
        try {
            return requireCache().getTable(dbName, tableName);
        } catch (StarRocksConnectorException e) {
            LOG.warn("Failed to fetch table {}.{} from catalog {}: {}",
                    dbName, tableName, context.getCatalogName(), e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           ConnectorMetadatRequestContext requestContext) {
        // Partition metadata will be provided in later iterations.
        return ConnectorMetadata.super.listPartitionNames(databaseName, tableName, requestContext);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        if (config == null) {
            return null;
        }
        // object_store 模式在后续迭代中使用 Storage Volume 等配置。
        // 目前返回 null，表示没有额外的云存储配置。
        LOG.debug("fetchMode={} for starrocks catalog {}", config.getFetchMode(), context.getCatalogName());
        return null;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        return ConnectorMetadata.super.tableExists(context, dbName, tblName);
    }

    @Override
    public void close() {
        if (metadataCache != null) {
            metadataCache.close();
        }
    }
}

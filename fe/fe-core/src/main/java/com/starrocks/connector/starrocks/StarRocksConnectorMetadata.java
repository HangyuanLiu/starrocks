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

import com.google.common.base.Strings;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.http.rest.v2.vo.PartitionInfoView;
import com.starrocks.http.rest.v2.vo.TableSchemaView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
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
    public Table.TableType getTableType() {
        return Table.TableType.STARROCKS;
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        try {
            if (listDbNames(context).contains(name)) {
                return new Database(0, name);
            } else {
                return null;
            }
        } catch (StarRocksConnectorException e) {
            LOG.warn("Failed to get database {} from catalog {}: {}",
                    name, this.context.getCatalogName(), e.getMessage());
            return null;
        }
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

    public long beginTransaction(String dbName, String tableName, String label, int timeoutSecs) {
        LOG.info("StarRocks REST beginTransaction request: catalog={}, db={}, table={}, label={}, timeoutSec={}",
                context.getCatalogName(), dbName, tableName, label, timeoutSecs);
        StarRocksRestClient.TransactionResult result =
                requireCache().beginTransaction(dbName, tableName, label, timeoutSecs);
        if (result == null || !result.isOk()) {
            String message = result != null ? result.getMessage() : "null response";
            throw new StarRocksConnectorException("Begin transaction failed: " + message);
        }
        Long txnId = result.getTxnId();
        if (txnId == null) {
            throw new StarRocksConnectorException("Begin transaction returned empty txn id");
        }
        LOG.info("StarRocks REST beginTransaction response: catalog={}, db={}, table={}, label={}, txnId={}",
                context.getCatalogName(), dbName, tableName, label, txnId);
        return txnId;
    }

    public TableSchemaView getTableSchemaView(String dbName, String tableName) {
        return requireCache().getTableSchemaView(dbName, tableName);
    }

    public List<PartitionInfoView.PartitionView> listTablePartitions(String dbName, String tableName) {
        return requireCache().listTablePartitions(dbName, tableName);
    }

    @Override
    public void finishSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos, String branch) {
        List<TSinkCommitInfo> infos = commitInfos == null ? Collections.emptyList() : commitInfos;
        String label = resolveLabel(infos);
        if (Strings.isNullOrEmpty(label)) {
            throw new StarRocksConnectorException("Missing label for StarRocks sink commit");
        }
        List<TTabletCommitInfo> tabletCommitInfos = extractTabletCommitInfos(infos);
        StarRocksRestClient.TransactionResult prepareResult =
                requireCache().prepareTransaction(dbName, label, tabletCommitInfos, Collections.emptyList());
        if (prepareResult == null || !prepareResult.isOk()) {
            String message = prepareResult != null ? prepareResult.getMessage() : "null response";
            throw new StarRocksConnectorException("Prepare transaction failed: " + message);
        }
        StarRocksRestClient.TransactionResult commitResult = requireCache().commitTransaction(dbName, label);
        if (commitResult == null || !commitResult.isOk()) {
            String message = commitResult != null ? commitResult.getMessage() : "null response";
            throw new StarRocksConnectorException("Commit transaction failed: " + message);
        }
    }

    @Override
    public void finishSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos, String branch, Object extra) {
        finishSink(dbName, tableName, commitInfos, branch);
    }

    @Override
    public void abortSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos) {
        rollbackTransaction(dbName, tableName, null, commitInfos);
    }

    public void rollbackTransaction(String dbName, String tableName, String label, List<TSinkCommitInfo> commitInfos) {
        List<TSinkCommitInfo> infos = commitInfos == null ? Collections.emptyList() : commitInfos;
        String resolvedLabel = Strings.isNullOrEmpty(label) ? resolveLabel(infos) : label;
        if (Strings.isNullOrEmpty(resolvedLabel)) {
            throw new StarRocksConnectorException("Missing label for StarRocks transaction rollback");
        }
        List<TTabletCommitInfo> tabletCommitInfos = extractTabletCommitInfos(infos);
        List<TTabletFailInfo> failedTablets = new ArrayList<>();
        for (TTabletCommitInfo commitInfo : tabletCommitInfos) {
            if (commitInfo == null) {
                continue;
            }
            TTabletFailInfo failInfo = new TTabletFailInfo();
            failInfo.setTabletId(commitInfo.getTabletId());
            failInfo.setBackendId(commitInfo.getBackendId());
            failedTablets.add(failInfo);
        }
        StarRocksRestClient.TransactionResult result =
                requireCache().rollbackTransaction(dbName, resolvedLabel, failedTablets);
        if (result == null || !result.isOk()) {
            String message = result != null ? result.getMessage() : "null response";
            throw new StarRocksConnectorException("Rollback transaction failed: " + message);
        }
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

    private static String resolveLabel(List<TSinkCommitInfo> commitInfos) {
        String label = null;
        for (TSinkCommitInfo info : commitInfos) {
            if (info == null || !info.isSetStarrocks_label()) {
                continue;
            }
            String candidate = info.getStarrocks_label();
            if (label == null) {
                label = candidate;
            } else if (!label.equals(candidate)) {
                throw new StarRocksConnectorException("Inconsistent starrocks label in commit infos");
            }
        }
        return label;
    }

    private static List<TTabletCommitInfo> extractTabletCommitInfos(List<TSinkCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.isEmpty()) {
            return Collections.emptyList();
        }
        List<TTabletCommitInfo> tabletCommitInfos = new ArrayList<>();
        for (TSinkCommitInfo info : commitInfos) {
            if (info != null && info.isSetStarrocks_tablet_commit_info()) {
                tabletCommitInfos.add(info.getStarrocks_tablet_commit_info());
            }
        }
        return tabletCommitInfos;
    }
}

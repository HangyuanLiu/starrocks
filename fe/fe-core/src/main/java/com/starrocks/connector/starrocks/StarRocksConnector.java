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

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * StarRocks external catalog connector.
 *
 * <p>This connector acts as a Consumer to another StarRocks cluster
 * and is the entry point for RPC/object_store fetch modes.</p>
 */
public class StarRocksConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(StarRocksConnector.class);

    private final ConnectorContext context;
    private final StarRocksConnectorMetadata metadata;
    private StarRocksConnectorConfig connectorConfig;

    public StarRocksConnector(ConnectorContext context) {
        this.context = context;
        this.metadata = new StarRocksConnectorMetadata(context);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        ensureConfigBound();
        return metadata;
    }

    @Override
    public void bindConfig(ConnectorConfig config) {
        if (!(config instanceof StarRocksConnectorConfig)) {
            throw new StarRocksConnectorException(
                    "Unexpected config class '" + (config == null ? "null" : config.getClass().getName()) + "'");
        }
        this.connectorConfig = (StarRocksConnectorConfig) config;
        metadata.bindConfig(this.connectorConfig);
        LOG.info("StarRocks connector [{}] bound config with fetch mode {}",
                context.getCatalogName(), connectorConfig.getFetchMode());
    }

    private void ensureConfigBound() {
        if (connectorConfig == null) {
            throw new StarRocksConnectorException(
                    "StarRocks catalog '" + context.getCatalogName() + "' is not initialized, config not bound");
        }
    }

    @Override
    public void shutdown() {
        metadata.close();
    }
}

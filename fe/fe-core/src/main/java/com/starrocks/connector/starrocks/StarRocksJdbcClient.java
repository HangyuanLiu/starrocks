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
import com.starrocks.common.Config;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Lightweight JDBC client used for fetching database and table metadata from the remote StarRocks FE.
 */
public interface StarRocksJdbcClient extends Closeable {

    Logger LOG = LogManager.getLogger(StarRocksJdbcClient.class);

    static StarRocksJdbcClient create(StarRocksConnectorConfig config) {
        return new DefaultStarRocksJdbcClient(config);
    }

    List<String> listDatabases();

    List<String> listTables(String dbName);

    @Override
    void close();

    final class DefaultStarRocksJdbcClient implements StarRocksJdbcClient {
        private static final String MYSQL_PREFIX = "jdbc:mysql";
        private static final String MARIADB_PREFIX = "jdbc:mariadb";

        private final HikariDataSource dataSource;

        DefaultStarRocksJdbcClient(StarRocksConnectorConfig config) {
            Preconditions.checkNotNull(config, "config is null");
            try {
                Class.forName("org.mariadb.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                throw new StarRocksConnectorException("Failed to load MariaDB JDBC driver", e);
            }
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(normalizeJdbcUrl(config.getFeJdbcUrl()));
            hikariConfig.setUsername(config.getUser());
            hikariConfig.setPassword(config.getPassword());
            hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
            hikariConfig.setMaximumPoolSize(Config.jdbc_connection_pool_size);
            hikariConfig.setMinimumIdle(Config.jdbc_minimum_idle_connections);
            hikariConfig.setIdleTimeout(Config.jdbc_connection_idle_timeout_ms);
            hikariConfig.setPoolName("starrocks-catalog-jdbc-" + System.identityHashCode(this));
            dataSource = new HikariDataSource(hikariConfig);
        }

        private static String normalizeJdbcUrl(String jdbcUrl) {
            Preconditions.checkNotNull(jdbcUrl, "jdbcUrl is null");
            if (jdbcUrl.toLowerCase(Locale.ROOT).startsWith(MYSQL_PREFIX)) {
                return MARIADB_PREFIX + jdbcUrl.substring(MYSQL_PREFIX.length());
            }
            return jdbcUrl;
        }

        @Override
        public List<String> listDatabases() {
            final String sql = "SHOW DATABASES";
            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                List<String> databases = new ArrayList<>();
                while (resultSet.next()) {
                    databases.add(resultSet.getString(1));
                }
                return ImmutableList.copyOf(databases);
            } catch (SQLException e) {
                throw new StarRocksConnectorException("Failed to list databases from starrocks catalog", e);
            }
        }

        @Override
        public List<String> listTables(String dbName) {
            Preconditions.checkNotNull(dbName, "dbName is null");
            final String sql = "SELECT TABLE_NAME FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY TABLE_NAME";
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, dbName);
                try (ResultSet resultSet = ps.executeQuery()) {
                    List<String> tables = new ArrayList<>();
                    while (resultSet.next()) {
                        tables.add(resultSet.getString(1));
                    }
                    return ImmutableList.copyOf(tables);
                }
            } catch (SQLException e) {
                throw new StarRocksConnectorException(
                        String.format("Failed to list tables for database '%s'", dbName), e);
            }
        }

        @Override
        public void close() {
            dataSource.close();
        }
    }
}

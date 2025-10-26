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

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class StarRocksConnectorTest {

    private static final Map<String, String> BASE_PROPERTIES = ImmutableMap.<String, String>builder()
            .put(StarRocksConnectorConfig.KEY_FE_HTTP_URL, "http://127.0.0.1:8030")
            .put(StarRocksConnectorConfig.KEY_FE_JDBC_URL, "jdbc:mysql://127.0.0.1:9030")
            .put(StarRocksConnectorConfig.KEY_USER, "test_user")
            .put(StarRocksConnectorConfig.KEY_PASSWORD, "")
            .put(StarRocksConnectorConfig.KEY_BE_RPC_ENDPOINTS, "127.0.0.1:9060")
            .build();

    @Test
    public void testConnectorTypeRegistration() {
        Assertions.assertTrue(ConnectorType.isSupport("starrocks"));
        Assertions.assertEquals(ConnectorType.STARROCKS, ConnectorType.from("starrocks"));
    }

    @Test
    public void testCreateStarRocksConnector() throws StarRocksConnectorException {
        ConnectorContext context =
                new ConnectorContext("sr_external", "starrocks", BASE_PROPERTIES);
        CatalogConnector catalogConnector = ConnectorFactory.createConnector(context, false);
        Assertions.assertNotNull(catalogConnector);
        Assertions.assertEquals("StarRocksConnector", catalogConnector.normalConnectorClassName());
    }
}

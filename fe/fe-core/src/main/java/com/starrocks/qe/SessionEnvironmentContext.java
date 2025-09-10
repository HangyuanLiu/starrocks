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

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.DdlException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.starrocks.common.util.Util.normalizeName;

public class SessionEnvironmentContext {
    private static final Logger LOG = LogManager.getLogger(SessionEnvironmentContext.class);

    private final ConnectContext context;

    private String currentDb = "";
    private ComputeResource computeResource = null;
    private com.starrocks.thrift.TWorkGroup resourceGroup;
    // the protocol capability which server say it can support
    private com.starrocks.mysql.MysqlCapability serverCapability = com.starrocks.mysql.MysqlCapability.DEFAULT_CAPABILITY;
    // the protocol capability after server and client negotiate
    private com.starrocks.mysql.MysqlCapability capability;
    // Serializer used to pack MySQL packet.
    private com.starrocks.mysql.MysqlSerializer serializer;

    public SessionEnvironmentContext(ConnectContext context) {
        this.context = context;
    }

    // Current catalog of this session is stored in session variables
    public String getCurrentCatalog() {
        return context.getSessionVariable().getCatalog();
    }

    public void setCurrentCatalog(String currentCatalog) {
        context.getSessionVariable().setCatalog(currentCatalog);
    }

    public String getDatabase() {
        return currentDb;
    }

    public void setDatabase(String db) {
        currentDb = db;
    }

    // Change current catalog of this session, and reset current database.
    // We can support "use 'catalog <catalog_name>'" from mysql client or
    // "use catalog <catalog_name>" from JDBC.
    public void changeCatalog(String newCatalogName) throws DdlException {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        CatalogMgr catalogMgr = globalStateMgr.getCatalogMgr();
        newCatalogName = normalizeName(newCatalogName);
        if (!catalogMgr.catalogExists(newCatalogName)) {
            com.starrocks.common.ErrorReport.reportDdlException(
                    com.starrocks.common.ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
        }
        if (!CatalogMgr.isInternalCatalog(newCatalogName)) {
            try {
                Authorizer.checkAnyActionOnCatalog(context, newCatalogName);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(newCatalogName, context.getCurrentUserIdentity(),
                        context.getCurrentRoleIds(), PrivilegeType.ANY.name(), ObjectType.CATALOG.name(), newCatalogName);
            }
        }
        this.setCurrentCatalog(newCatalogName);
        this.setDatabase("");
    }

    // Change current catalog and database of this session.
    // identifier could be "CATALOG.DB" or "DB".
    // For "CATALOG.DB", we change the current catalog and database.
    // For "DB", we keep the current catalog and change the current database.
    public void changeCatalogDb(String identifier) throws DdlException {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        CatalogMgr catalogMgr = globalStateMgr.getCatalogMgr();
        MetadataMgr metadataMgr = globalStateMgr.getMetadataMgr();

        String dbName;

        String[] parts = identifier.split("\\.", 2);
        if (parts.length != 1 && parts.length != 2) {
            com.starrocks.common.ErrorReport.reportDdlException(
                    com.starrocks.common.ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
        }

        if (parts.length == 1) {
            dbName = identifier;
        } else {
            String newCatalogName = normalizeName(parts[0]);
            if (!catalogMgr.catalogExists(newCatalogName)) {
                com.starrocks.common.ErrorReport.reportDdlException(
                        com.starrocks.common.ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
            }
            if (!CatalogMgr.isInternalCatalog(newCatalogName)) {
                try {
                    Authorizer.checkAnyActionOnCatalog(context, newCatalogName);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(newCatalogName, context.getCurrentUserIdentity(),
                            context.getCurrentRoleIds(), PrivilegeType.ANY.name(), ObjectType.CATALOG.name(), newCatalogName);
                }
            }
            this.setCurrentCatalog(newCatalogName);
            dbName = parts[1];
        }

        dbName = normalizeName(dbName);

        if (!Strings.isNullOrEmpty(dbName) && metadataMgr.getDb(context, this.getCurrentCatalog(), dbName) == null) {
            LOG.debug("Unknown catalog {} and db {}", this.getCurrentCatalog(), dbName);
            com.starrocks.common.ErrorReport.reportDdlException(
                    com.starrocks.common.ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        try {
            Authorizer.checkAnyActionOnOrInDb(context, this.getCurrentCatalog(), dbName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(this.getCurrentCatalog(), context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(), PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), dbName);
        }

        this.setDatabase(dbName);
    }

    public long getCurrentWarehouseId() {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        String warehouseName = context.getSessionVariable().getWarehouseName();
        if (warehouseName.equalsIgnoreCase(WarehouseManager.DEFAULT_WAREHOUSE_NAME)) {
            return WarehouseManager.DEFAULT_WAREHOUSE_ID;
        }

        Warehouse warehouse = globalStateMgr.getWarehouseMgr().getWarehouseAllowNull(warehouseName);
        if (warehouse == null) {
            throw new SemanticException("Warehouse " + warehouseName + " not exist");
        }
        return warehouse.getId();
    }

    public String getCurrentWarehouseName() {
        return context.getSessionVariable().getWarehouseName();
    }

    public void setCurrentWarehouse(String currentWarehouse) {
        context.getSessionVariable().setWarehouseName(currentWarehouse);
        this.computeResource = null;
    }

    public void setCurrentWarehouseId(long warehouseId) {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        Warehouse warehouse = globalStateMgr.getWarehouseMgr().getWarehouse(warehouseId);
        context.getSessionVariable().setWarehouseName(warehouse.getName());
        this.computeResource = null;
    }

    public void setCurrentComputeResource(ComputeResource computeResource) {
        this.computeResource = computeResource;
    }

    // once warehouse is set, needs to choose the available cngroup
    // try to acquire cn group id once the warehouse is set
    public synchronized void tryAcquireResource(boolean force) {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        if (!force && this.computeResource != null) {
            return;
        }
        final long warehouseId = this.getCurrentWarehouseId();
        final WarehouseManager warehouseManager = globalStateMgr.getWarehouseMgr();
        this.computeResource = warehouseManager.acquireComputeResource(warehouseId, this.computeResource);
    }

    public ComputeResource getCurrentComputeResource() {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseManager.DEFAULT_RESOURCE;
        }
        if (this.computeResource == null) {
            tryAcquireResource(false);
        }
        return this.computeResource;
    }

    public String getCurrentComputeResourceName() {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        if (!RunMode.isSharedDataMode() || this.computeResource == null) {
            return "";
        }
        final WarehouseManager warehouseManager = globalStateMgr.getWarehouseMgr();
        return warehouseManager.getComputeResourceName(this.computeResource);
    }

    public ComputeResource getCurrentComputeResourceNoAcquire() {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseManager.DEFAULT_RESOURCE;
        }
        return this.computeResource;
    }

    public com.starrocks.thrift.TWorkGroup getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(com.starrocks.thrift.TWorkGroup resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public com.starrocks.mysql.MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public void setServerCapability(com.starrocks.mysql.MysqlCapability serverCapability) {
        this.serverCapability = serverCapability;
    }

    public com.starrocks.mysql.MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(com.starrocks.mysql.MysqlCapability capability) {
        this.capability = capability;
    }

    public com.starrocks.mysql.MysqlSerializer getSerializer() {
        return serializer;
    }

    public void setSerializer(com.starrocks.mysql.MysqlSerializer serializer) {
        this.serializer = serializer;
    }
}



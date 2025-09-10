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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContext.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationContext;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AuthorizationContext;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.ssl.SSLChannel;
import com.starrocks.mysql.ssl.SSLChannelImp;
import com.starrocks.mysql.ssl.SSLContextLoader;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.spm.SQLPlanStorage;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

// When one client connect in, we create a connection context for it.
// We store session information here. Meanwhile, ConnectScheduler all
// connect with its connection id.
public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);
    protected static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<>();

    // Session connection (channel, remote ip, session id, connection id)
    protected SessionConnectionContext sessionConnectionContext = new SessionConnectionContext();

    // Execution context (stmt/query/connection identifiers, timings, flags)
    protected SessionExecutionContext sessionExecutionContext = new SessionExecutionContext();

    // Session environment (catalog, db, warehouse, resource group)
    protected SessionEnvironmentContext sessionEnvironmentContext = new SessionEnvironmentContext(this);

    // Authentication context that encapsulates authentication and authorization information
    protected AuthenticationContext authenticationContext = new AuthenticationContext();

    // Authorization context that encapsulates current role ids and groups
    protected AuthorizationContext authorizationContext = new AuthorizationContext();

    // Session and user variables encapsulated in SessionVariableContext
    protected SessionVariableContext sessionVariableContext;

    private ConnectContext parent;

    // Cache thread info for this connection.
    protected ThreadInfo threadInfo;

    protected GlobalStateMgr globalStateMgr;

    public void setTxnId(long txnId) {
        sessionExecutionContext.setTxnId(txnId);
    }

    public long getTxnId() {
        return sessionExecutionContext.getTxnId();
    }

    public StmtExecutor getExecutor() {
        return sessionExecutionContext.getExecutor();
    }

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public static SessionVariable getSessionVariableOrDefault() {
        ConnectContext ctx = get();
        return (ctx != null) ? ctx.getSessionVariable() : SessionVariable.DEFAULT_SESSION_VARIABLE;
    }

    public static void remove() {
        threadLocalInfo.remove();
    }

    public boolean isQueryStmt(StatementBase statement) {
        if (statement instanceof QueryStatement) {
            return true;
        }

        if (statement instanceof ExecuteStmt) {
            ExecuteStmt executeStmt = (ExecuteStmt) statement;
            PrepareStmtContext prepareStmtContext = getPreparedStmt(executeStmt.getStmtName());
            if (prepareStmtContext != null) {
                return prepareStmtContext.getStmt().getInnerStmt() instanceof QueryStatement;
            }
        }
        return false;
    }

    public ConnectContext() {
        this(null);
    }

    public ConnectContext(StreamConnection connection) {
        // `globalStateMgr` is used in many cases, so we should explicitly make sure it is not null
        globalStateMgr = GlobalStateMgr.getCurrentState();
        sessionConnectionContext.setClosed(false);
        sessionExecutionContext.setState(new QueryState());
        sessionExecutionContext.resetReturnRows();
        sessionEnvironmentContext.setServerCapability(MysqlCapability.DEFAULT_CAPABILITY);
        sessionExecutionContext.setKilled(false);
        sessionEnvironmentContext.setSerializer(MysqlSerializer.newInstance());
        sessionVariableContext = new SessionVariableContext(() -> globalStateMgr.getVariableMgr().newSessionVariable());
        sessionExecutionContext.setCommand(MysqlCommand.COM_SLEEP);
        sessionExecutionContext.setQueryDetail(null);

        if (shouldDumpQuery()) {
            sessionExecutionContext.setDumpInfo(new QueryDumpInfo(this));
        }
        sessionConnectionContext.setSessionId(UUIDUtil.genUUID());

        MysqlChannel mysqlChannel = new MysqlChannel(connection);
        sessionConnectionContext.setMysqlChannel(mysqlChannel);
        if (connection != null) {
            sessionConnectionContext.setRemoteIP(mysqlChannel.getRemoteIp());
        }
    }

    /**
     * Build a ConnectContext for normal query.
     */
    public static ConnectContext build() {
        return new ConnectContext();
    }

    /**
     * Build a ConnectContext for inner query which is used for StarRocks internal query.
     */
    public static ConnectContext buildInner() {
        ConnectContext connectContext = new ConnectContext();
        // disable materialized view rewrite for inner query
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        return connectContext;
    }

    public SQLPlanStorage getSqlPlanStorage() {
        return sessionExecutionContext.getSqlPlanStorage();
    }

    public void putPreparedStmt(String stmtName, PrepareStmtContext ctx) {
        sessionExecutionContext.putPreparedStmt(stmtName, ctx);
    }

    public PrepareStmtContext getPreparedStmt(String stmtName) {
        return sessionExecutionContext.getPreparedStmt(stmtName);
    }

    public void removePreparedStmt(String stmtName) {
        sessionExecutionContext.removePreparedStmt(stmtName);
    }

    public long getStmtId() {
        return sessionExecutionContext.getStmtId();
    }

    public void setStmtId(long stmtId) {
        sessionExecutionContext.setStmtId(stmtId);
    }

    public long getForwardedStmtId() {
        return sessionExecutionContext.getForwardedStmtId();
    }

    public void setForwardedStmtId(long forwardedStmtId) {
        sessionExecutionContext.setForwardedStmtId(forwardedStmtId);
    }

    public String getRemoteIP() {
        return sessionConnectionContext.getRemoteIP();
    }

    public void setRemoteIP(String remoteIP) {
        sessionConnectionContext.setRemoteIP(remoteIP);
    }

    public void setQueryDetail(QueryDetail queryDetail) {
        sessionExecutionContext.setQueryDetail(queryDetail);
    }

    public QueryDetail getQueryDetail() {
        return sessionExecutionContext.getQueryDetail();
    }

    public void setAuditEventBuilder(AuditEventBuilder auditEventBuilder) {
        sessionExecutionContext.setAuditEventBuilder(auditEventBuilder);
    }

    public AuditEventBuilder getAuditEventBuilder() {
        return sessionExecutionContext.getAuditEventBuilder();
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public Optional<Boolean> getUseConnectorMetadataCache() {
        return sessionVariableContext.getUseConnectorMetadataCache();
    }

    public void setUseConnectorMetadataCache(Optional<Boolean> useConnectorMetadataCache) {
        sessionVariableContext.setUseConnectorMetadataCache(useConnectorMetadataCache);
    }

    public static ConnectContext exchangeThreadLocalInfo(ConnectContext ctx) {
        ConnectContext prev = threadLocalInfo.get();
        threadLocalInfo.set(ctx);
        return prev;
    }

    public void setGlobalStateMgr(GlobalStateMgr globalStateMgr) {
        Preconditions.checkState(globalStateMgr != null);
        this.globalStateMgr = globalStateMgr;
    }

    public GlobalStateMgr getGlobalStateMgr() {
        return globalStateMgr;
    }

    public String getQualifiedUser() {
        return authenticationContext.getQualifiedUser();
    }

    public void setQualifiedUser(String qualifiedUser) {
        authenticationContext.setQualifiedUser(qualifiedUser);
    }

    public UserIdentity getCurrentUserIdentity() {
        return authenticationContext.getCurrentUserIdentity();
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        authenticationContext.setCurrentUserIdentity(currentUserIdentity);
    }

    public void setDistinguishedName(String distinguishedName) {
        authenticationContext.setDistinguishedName(distinguishedName);
    }

    public String getDistinguishedName() {
        return authenticationContext.getDistinguishedName();
    }

    public Set<Long> getCurrentRoleIds() {
        return authorizationContext.getCurrentRoleIds();
    }

    public void setCurrentRoleIds(UserIdentity user) {
        if (user.isEphemeral()) {
            authorizationContext.setCurrentRoleIds(new HashSet<>());
        } else {
            try {
                Set<Long> defaultRoleIds;
                if (GlobalVariable.isActivateAllRolesOnLogin()) {
                    defaultRoleIds = globalStateMgr.getAuthorizationMgr().getRoleIdsByUser(user);
                } else {
                    defaultRoleIds = globalStateMgr.getAuthorizationMgr().getDefaultRoleIdsByUser(user);
                }
                authorizationContext.setCurrentRoleIds(defaultRoleIds);
            } catch (PrivilegeException e) {
                LOG.warn("Set current role fail : {}", e.getMessage());
            }
        }
    }

    public void setCurrentRoleIds(Set<Long> roleIds) {
        authorizationContext.setCurrentRoleIds(roleIds);
    }

    public void setCurrentRoleIds(UserIdentity userIdentity, Set<String> groups) {
        setCurrentRoleIds(userIdentity);
    }

    public void setAuthInfoFromThrift(TAuthInfo authInfo) {
        if (authInfo.isSetCurrent_user_ident()) {
            setAuthInfoFromThrift(authInfo.getCurrent_user_ident());
        } else {
            setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip));
            setCurrentRoleIds(getCurrentUserIdentity());
        }
    }

    public void setAuthInfoFromThrift(TUserIdentity tUserIdent) {
        setCurrentUserIdentity(UserIdentityUtils.fromThrift(tUserIdent));
        if (tUserIdent.isSetCurrent_role_ids()) {
            setCurrentRoleIds(new HashSet<>(tUserIdent.current_role_ids.getRole_id_list()));
        } else {
            setCurrentRoleIds(getCurrentUserIdentity());
        }
    }

    public Set<String> getGroups() {
        return authorizationContext.getGroups();
    }

    public void setGroups(Set<String> groups) {
        authorizationContext.setGroups(groups);
    }

    public String getAuthToken() {
        return authenticationContext.getAuthToken();
    }

    public void setAuthToken(String authToken) {
        authenticationContext.setAuthToken(authToken);
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return authenticationContext.getAuthenticationProvider();
    }

    public void setAuthPlugin(String authPlugin) {
        authenticationContext.setAuthPlugin(authPlugin);
    }

    public String getAuthPlugin() {
        return authenticationContext.getAuthPlugin();
    }

    public void setAuthDataSalt(byte[] authDataSalt) {
        authenticationContext.setAuthDataSalt(authDataSalt);
    }

    public String getSecurityIntegration() {
        return authenticationContext.getSecurityIntegration();
    }

    public void setSecurityIntegration(String securityIntegration) {
        authenticationContext.setSecurityIntegration(securityIntegration);
    }

    /**
     * Get the authentication context for this connection
     */
    public AuthenticationContext getAuthenticationContext() {
        return authenticationContext;
    }

    public void modifyUserVariable(UserVariable userVariable) {
        sessionVariableContext.modifyUserVariable(userVariable);
    }

    /**
     * 1. The {@link ConnectContext#userVariables} in the current session should not be modified
     * until you are sure that the set sql was executed successfully.
     * 2. Changes to user variables during set sql execution should
     * be effected in the {@link ConnectContext#userVariablesCopyInWrite}.
     */
    public void modifyUserVariableCopyInWrite(UserVariable userVariable) {
        sessionVariableContext.modifyUserVariableCopyInWrite(userVariable);
    }

    /**
     * The SQL execution that sets the variable must reset userVariablesCopyInWrite when it finishes,
     * either normally or abnormally.
     * <p>
     * This method needs to be called at the time of setting the user variable.
     * call by {@link SetExecutor#execute()}, {@link StmtExecutor#processQueryScopeHint()}
     */
    public void resetUserVariableCopyInWrite() {
        sessionVariableContext.resetUserVariableCopyInWrite();
    }

    /**
     * After the successful execution of the SQL that set the variable,
     * the result of the change to the copy of userVariables is set back to the current session.
     * <p>
     * call by {@link SetExecutor#execute()}, {@link StmtExecutor#processQueryScopeHint()}
     */
    public void modifyUserVariables(Map<String, UserVariable> userVarCopyInWrite) {
        sessionVariableContext.modifyUserVariables(userVarCopyInWrite);
    }

    /**
     * Instead of using {@link ConnectContext#userVariables} when set userVariables,
     * use a copy of it, the purpose of which is to ensure atomicity/isolation of modifications to userVariables
     * <p>
     * This method needs to be called at the time of setting the user variable.
     * call by {@link SetExecutor#execute()}, {@link StmtExecutor#processQueryScopeHint()}
     */
    public void modifyUserVariablesCopyInWrite(Map<String, UserVariable> userVariables) {
        sessionVariableContext.modifyUserVariablesCopyInWrite(userVariables);
    }

    public SetStmt getModifiedSessionVariables() {
        return sessionVariableContext.getModifiedSessionVariables();
    }

    public void addModifiedSessionVariables(SystemVariable systemVariable) {
        sessionVariableContext.addModifiedSessionVariables(systemVariable);
    }

    public SessionVariable getSessionVariable() {
        return sessionVariableContext.getSessionVariable();
    }

    public SessionVariableContext getSessionVariableContext() {
        return sessionVariableContext;
    }

    public Map<String, UserVariable> getUserVariables() {
        return sessionVariableContext.getUserVariables();
    }

    public UserVariable getUserVariable(String variable) {
        return sessionVariableContext.getUserVariable(variable);
    }

    public void resetSessionVariable() {
        sessionVariableContext.resetSessionVariable();
    }

    public UserVariable getUserVariableCopyInWrite(String variable) {
        return sessionVariableContext.getUserVariableCopyInWrite(variable);
    }

    public Map<String, UserVariable> getUserVariablesCopyInWrite() {
        return sessionVariableContext.getUserVariablesCopyInWrite();
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        sessionVariableContext.setSessionVariable(sessionVariable);
    }

    public MysqlCommand getCommand() {
        return sessionExecutionContext.getCommand();
    }

    public void setCommand(MysqlCommand command) {
        sessionExecutionContext.setCommand(command);
    }

    public long getStartTime() {
        return sessionExecutionContext.getStartTime();
    }

    public Instant getStartTimeInstant() {
        return sessionExecutionContext.getStartTimeInstant();
    }

    public void setStartTime() {
        sessionExecutionContext.setStartTime();
    }

    @VisibleForTesting
    public void setStartTime(Instant start) {
        sessionExecutionContext.setStartTime(start);
    }

    public void setEndTime() {
        sessionExecutionContext.setEndTime();
    }

    public Instant getEndTimeInstant() {
        return sessionExecutionContext.getEndTimeInstant();
    }

    public void updateReturnRows(int returnRows) {
        sessionExecutionContext.updateReturnRows(returnRows);
    }

    public long getReturnRows() {
        return sessionExecutionContext.getReturnRows();
    }

    public void resetReturnRows() {
        sessionExecutionContext.resetReturnRows();
    }

    public MysqlSerializer getSerializer() {
        return sessionEnvironmentContext.getSerializer();
    }

    public int getConnectionId() {
        return sessionConnectionContext.getConnectionId();
    }

    public void setConnectionId(int connectionId) {
        sessionConnectionContext.setConnectionId(connectionId);
    }

    public String getProxyHostName() {
        return sessionConnectionContext.getProxyHostName();
    }

    public void setProxyHostName(String address) {
        sessionConnectionContext.setProxyHostName(address);
    }

    public boolean hasPendingForwardRequest() {
        return sessionExecutionContext.hasPendingForwardRequest();
    }

    public void incPendingForwardRequest() {
        sessionExecutionContext.incPendingForwardRequest();
    }

    public void decPendingForwardRequest() {
        sessionExecutionContext.decPendingForwardRequest();
    }

    public void resetConnectionStartTime() {
        sessionExecutionContext.resetConnectionStartTime();
    }

    public long getConnectionStartTime() {
        return sessionExecutionContext.getConnectionStartTime();
    }

    public MysqlChannel getMysqlChannel() {
        return sessionConnectionContext.getMysqlChannel();
    }

    public QueryState getState() {
        return sessionExecutionContext.getState();
    }

    public void setState(QueryState state) {
        sessionExecutionContext.setState(state);
    }

    public String getNormalizedErrorCode() {
        // TODO: how to unify TStatusCode, ErrorCode, ErrType, ConnectContext.errorCode
        if (StringUtils.isNotEmpty(sessionExecutionContext.getErrorCode())) {
            // error happens in BE execution.
            return sessionExecutionContext.getErrorCode();
        }

        if (getState().getErrType() != QueryState.ErrType.UNKNOWN) {
            // error happens in FE execution.
            return getState().getErrType().name();
        }

        return "";
    }

    public void resetErrorCode() {
        sessionExecutionContext.resetErrorCode();
    }

    public void setErrorCodeOnce(String errorCode) {
        sessionExecutionContext.setErrorCodeOnce(errorCode);
    }

    public MysqlCapability getCapability() {
        return sessionEnvironmentContext.getCapability();
    }

    public void setCapability(MysqlCapability capability) {
        sessionEnvironmentContext.setCapability(capability);
    }

    public MysqlCapability getServerCapability() {
        return sessionEnvironmentContext.getServerCapability();
    }

    public String getDatabase() {
        return sessionEnvironmentContext.getDatabase();
    }

    public void setDatabase(String db) {
        sessionEnvironmentContext.setDatabase(db);
    }

    public void setExecutor(StmtExecutor executor) {
        sessionExecutionContext.setExecutor(executor);
    }

    public synchronized void cleanup() {
        if (sessionConnectionContext.isClosed()) {
            return;
        }
        sessionConnectionContext.setClosed(true);
        sessionConnectionContext.getMysqlChannel().close();
        threadLocalInfo.remove();
        sessionExecutionContext.resetReturnRows();
        sessionEnvironmentContext.setCurrentComputeResource(null);
    }

    public boolean isKilled() {
        return (parent != null && parent.isKilled()) || sessionExecutionContext.isKilled();
    }

    // Set kill flag to true;
    public void setKilled() {
        sessionExecutionContext.setKilled(true);
    }

    public TUniqueId getExecutionId() {
        return sessionExecutionContext.getExecutionId();
    }

    public void setExecutionId(TUniqueId executionId) {
        sessionExecutionContext.setExecutionId(executionId);
    }

    public UUID getQueryId() {
        return sessionExecutionContext.getQueryId();
    }

    public void setQueryId(UUID queryId) {
        sessionExecutionContext.setQueryId(queryId);
    }

    public UUID getLastQueryId() {
        return sessionExecutionContext.getLastQueryId();
    }

    public void setLastQueryId(UUID queryId) {
        sessionExecutionContext.setLastQueryId(queryId);
    }

    public String getCustomQueryId() {
        return getSessionVariable() != null ? getSessionVariable().getCustomQueryId() : "";
    }

    public boolean isProfileEnabled() {
        if (getSessionVariable() == null) {
            return false;
        }
        if (getSessionVariable().isEnableProfile()) {
            return true;
        }
        if (!getSessionVariable().isEnableBigQueryProfile()) {
            return false;
        }
        return System.currentTimeMillis() - getStartTime() >
                getSessionVariable().getBigQueryProfileMilliSecondThreshold();
    }

    public boolean needMergeProfile() {
        return getSessionVariable().getPipelineProfileLevel() < TPipelineProfileLevel.DETAIL.getValue();
    }

    public boolean getIsLastStmt() {
        return sessionExecutionContext.getIsLastStmt();
    }

    public void setIsLastStmt(boolean isLastStmt) {
        sessionExecutionContext.setIsLastStmt(isLastStmt);
    }

    public void setIsHTTPQueryDump(boolean isHTTPQueryDump) {
        sessionExecutionContext.setIsHTTPQueryDump(isHTTPQueryDump);
    }

    public boolean isHTTPQueryDump() {
        return sessionExecutionContext.isHTTPQueryDump();
    }

    public boolean shouldDumpQuery() {
        return this.isHTTPQueryDump() || getSessionVariable().getEnableQueryDump();
    }

    public DumpInfo getDumpInfo() {
        return sessionExecutionContext.getDumpInfo();
    }

    public void setDumpInfo(DumpInfo dumpInfo) {
        sessionExecutionContext.setDumpInfo(dumpInfo);
    }

    public Set<Long> getCurrentSqlDbIds() {
        return sessionExecutionContext.getCurrentSqlDbIds();
    }

    public void setCurrentSqlDbIds(Set<Long> currentSqlDbIds) {
        sessionExecutionContext.setCurrentSqlDbIds(currentSqlDbIds);
    }

    public StatementBase.ExplainLevel getExplainLevel() {
        return sessionVariableContext.getExplainLevel();
    }

    public void setExplainLevel(StatementBase.ExplainLevel explainLevel) {
        sessionVariableContext.setExplainLevel(explainLevel);
    }

    public TWorkGroup getResourceGroup() {
        return sessionEnvironmentContext.getResourceGroup();
    }

    public void setResourceGroup(TWorkGroup resourceGroup) {
        sessionEnvironmentContext.setResourceGroup(resourceGroup);
    }

    public String getCurrentCatalog() {
        return sessionEnvironmentContext.getCurrentCatalog();
    }

    public void setCurrentCatalog(String currentCatalog) {
        sessionEnvironmentContext.setCurrentCatalog(currentCatalog);
    }

    public long getCurrentWarehouseId() {
        return sessionEnvironmentContext.getCurrentWarehouseId();
    }

    public String getCurrentWarehouseName() {
        return sessionEnvironmentContext.getCurrentWarehouseName();
    }

    public void setCurrentWarehouse(String currentWarehouse) {
        sessionEnvironmentContext.setCurrentWarehouse(currentWarehouse);
    }

    public void setCurrentWarehouseId(long warehouseId) {
        sessionEnvironmentContext.setCurrentWarehouseId(warehouseId);
    }

    public void setCurrentComputeResource(ComputeResource computeResource) {
        sessionEnvironmentContext.setCurrentComputeResource(computeResource);
    }

    public synchronized void tryAcquireResource(boolean force) {
        sessionEnvironmentContext.tryAcquireResource(force);
    }

    /**
     * Get the current compute resource, acquire it if not set.
     * NOTE: This method will acquire compute resource if it is not set.
     *
     * @return: the current compute resource, or the default resource if not in shared data mode.
     */
    public ComputeResource getCurrentComputeResource() {
        return sessionEnvironmentContext.getCurrentComputeResource();
    }

    /**
     * Get the name of the current compute resource.
     * NOTE: this method will not acquire compute resource if it is not set.
     *
     * @return: the name of the current compute resource, or empty string if not set.
     */
    public String getCurrentComputeResourceName() {
        return sessionEnvironmentContext.getCurrentComputeResourceName();
    }

    /**
     * Get the current compute resource without acquiring it.
     *
     * @return: the current compute resource(null if not set), or the default resource if not in shared data mode.
     */
    public ComputeResource getCurrentComputeResourceNoAcquire() {
        return sessionEnvironmentContext.getCurrentComputeResourceNoAcquire();
    }

    public void setParentConnectContext(ConnectContext parent) {
        this.parent = parent;
    }

    public boolean isStatisticsConnection() {
        return sessionExecutionContext.isStatisticsConnection();
    }

    public void setStatisticsConnection(boolean statisticsConnection) {
        sessionExecutionContext.setStatisticsConnection(statisticsConnection);
    }

    public boolean isStatisticsJob() {
        return sessionExecutionContext.isStatisticsJob();
    }

    public void setStatisticsJob(boolean statisticsJob) {
        sessionExecutionContext.setStatisticsJob(statisticsJob);
    }

    public void setStatisticsContext(boolean isStatisticsContext) {
        sessionExecutionContext.setStatisticsContext(isStatisticsContext);
    }

    public boolean isNeedQueued() {
        return sessionExecutionContext.isNeedQueued();
    }

    public void setNeedQueued(boolean needQueued) {
        sessionExecutionContext.setNeedQueued(needQueued);
    }

    public boolean isBypassAuthorizerCheck() {
        return authorizationContext.isBypassAuthorizerCheck();
    }

    public void setBypassAuthorizerCheck(boolean value) {
        authorizationContext.setBypassAuthorizerCheck(value);
    }

    public ConnectContext getParent() {
        return parent;
    }

    public void setRelationAliasCaseInSensitive(boolean relationAliasCaseInsensitive) {
        sessionVariableContext.setRelationAliasCaseInSensitive(relationAliasCaseInsensitive);
    }

    public boolean isRelationAliasCaseInsensitive() {
        return sessionVariableContext.isRelationAliasCaseInsensitive();
    }

    public void setForwardTimes(int forwardTimes) {
        sessionExecutionContext.setForwardTimes(forwardTimes);
    }

    public int getForwardTimes() {
        return sessionExecutionContext.getForwardTimes();
    }

    public void setSessionId(UUID sessionId) {
        sessionConnectionContext.setSessionId(sessionId);
    }

    public UUID getSessionId() {
        return sessionConnectionContext.getSessionId();
    }

    public QueryMaterializationContext getQueryMVContext() {
        return sessionExecutionContext.getQueryMVContext();
    }

    public void setQueryMVContext(QueryMaterializationContext queryMVContext) {
        sessionExecutionContext.setQueryMVContext(queryMVContext);
    }

    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        sessionConnectionContext.getMysqlChannel().startAcceptQuery(this, connectProcessor);
    }

    public void suspendAcceptQuery() {
        sessionConnectionContext.getMysqlChannel().suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        sessionConnectionContext.getMysqlChannel().resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        sessionConnectionContext.getMysqlChannel().stopAcceptQuery();
    }

    // kill operation with no protect.
    public void kill(boolean killConnection, String cancelledMessage) {
        LOG.warn("kill query, {}, kill connection: {}",
                getMysqlChannel().getRemoteHostPortString(), killConnection);
        // Now, cancel running process.
        StmtExecutor executorRef = getExecutor();
        if (killConnection) {
            sessionExecutionContext.setKilled(true);
        }
        if (executorRef != null) {
            executorRef.cancel(cancelledMessage);
        }
        if (killConnection) {
            int times = 0;
            while (!sessionConnectionContext.isClosed()) {
                try {
                    Thread.sleep(10);
                    times++;
                    if (times > 100) {
                        LOG.warn("kill queryId={} connectId={} wait for close fail, break.", getQueryId(), getConnectionId());
                        break;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("sleep exception, ignore.", e);
                    break;
                }
            }
            // Close channel to break connection with client
            getMysqlChannel().close();
        }
    }

    /**
     * NOTE: The ExecTimeout should not contain the pending time which may be caused by QueryQueue's scheduler.
     * </p>
     *
     * @return Get the timeout for this session, unit: second
     */
    public int getExecTimeout() {
        return (int) (sessionExecutionContext.getPendingTimeSecond() + getExecTimeoutWithoutPendingTime());
    }

    private int getExecTimeoutWithoutPendingTime() {
        return getExecutor() != null ? getExecutor().getExecTimeout() : getSessionVariable().getQueryTimeoutS();
    }

    /**
     * update the pending time for this session, unit: second
     *
     * @param pendingTimeSecond: the pending time for this session
     */
    public void setPendingTimeSecond(int pendingTimeSecond) {
        sessionExecutionContext.setPendingTimeSecond(pendingTimeSecond);
    }

    public long getPendingTimeSecond() {
        return sessionExecutionContext.getPendingTimeSecond();
    }

    private String getExecType() {
        return getExecutor() != null ? getExecutor().getExecType() : "Query";
    }

    private boolean isExecLoadType() {
        return getExecutor() != null && getExecutor().isExecLoadType();
    }

    /**
     * Check the connect context is timeout or not. If true, kill the connection, otherwise, return false.
     *
     * @param now : current time in milliseconds
     * @return true if timeout, false otherwise
     */
    public boolean checkTimeout(long now) {
        long startTimeMillis = getStartTime();
        if (startTimeMillis <= 0) {
            return false;
        }

        long delta = now - startTimeMillis;
        boolean killFlag = false;
        boolean killConnection = false;
        String sql = "";
        if (getExecutor() != null) {
            sql = getExecutor().getOriginStmtInString();
        }
        String errMsg = "";
        if (getCommand() == MysqlCommand.COM_SLEEP) {
            int waitTimeout = getSessionVariable().getWaitTimeoutS();
            if (delta > waitTimeout * 1000L) {
                // Need kill this connection.
                LOG.warn("kill wait timeout connection, remote: {}, wait timeout: {}, query id: {}, sql: {}",
                        getMysqlChannel().getRemoteHostPortString(), waitTimeout, getQueryId(), SqlUtils.sqlPrefix(sql));

                killFlag = true;
                killConnection = true;

                errMsg = String.format("Connection reached its wait timeout of %d seconds", waitTimeout);
            }
        } else {
            long timeoutSecond = getExecTimeout();
            if (delta > timeoutSecond * 1000L) {
                final long pendingTime = getPendingTimeSecond();
                final long execTimeout = getExecTimeoutWithoutPendingTime();
                LOG.warn("kill timeout {}, remote: {}, execute timeout: {}, exec timeout: {}, pending time:{}, " +
                                "query id: {}, sql: {}",
                        getExecType().toLowerCase(), getMysqlChannel().getRemoteHostPortString(), timeoutSecond,
                        execTimeout, pendingTime, getQueryId(), SqlUtils.sqlPrefix(sql));

                // Only kill
                killFlag = true;

                String suggestedMsg = String.format("please increase the '%s' session variable, pending time:%s",
                        isExecLoadType() ? SessionVariable.INSERT_TIMEOUT : SessionVariable.QUERY_TIMEOUT, pendingTime);
                errMsg = ErrorCode.ERR_TIMEOUT.formatErrorMsg(getExecType(), execTimeout, suggestedMsg);
            }
        }
        if (killFlag) {
            kill(killConnection, errMsg);
        }
        return killFlag;
    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo() {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        return threadInfo;
    }

    public int getAliveBackendNumber() {
        int v = getSessionVariable().getCboDebugAliveBackendNumber();
        if (v > 0) {
            return v;
        }
        return globalStateMgr.getNodeMgr().getClusterInfo().getAliveBackendNumber();
    }

    public int getTotalBackendNumber() {
        return globalStateMgr.getNodeMgr().getClusterInfo().getTotalBackendNumber();
    }

    public int getAliveComputeNumber() {
        return globalStateMgr.getNodeMgr().getClusterInfo().getAliveComputeNodeNumber();
    }

    /**
     * BackendNode + ComputeNode
     */
    public int getAliveExecutionNodesNumber() {
        return getAliveBackendNumber() +
                (RunMode.isSharedDataMode() ?
                        getGlobalStateMgr().getNodeMgr().getClusterInfo().getAliveComputeNodeNumber() : 0);
    }

    public void setPending(boolean pending) {
        sessionExecutionContext.setPending(pending);
    }

    public boolean isPending() {
        return sessionExecutionContext.isPending();
    }

    public void setIsForward(boolean forward) {
        sessionExecutionContext.setIsForward(forward);
    }

    public boolean isForward() {
        return sessionExecutionContext.isForward();
    }

    public boolean enableSSL() throws IOException {
        SSLChannel sslChannel =
                new SSLChannelImp(SSLContextLoader.getSslContext().createSSLEngine(), sessionConnectionContext.getMysqlChannel());
        if (!sslChannel.init()) {
            return false;
        } else {
            sessionConnectionContext.getMysqlChannel().setSSLChannel(sslChannel);
            return true;
        }
    }

    public StmtExecutor executeSql(String sql) throws Exception {
        StatementBase sqlStmt = SqlParser.parse(sql, getSessionVariable()).get(0);
        sqlStmt.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = StmtExecutor.newInternalExecutor(this, sqlStmt);
        setExecutor(executor);
        setThreadLocalInfo();
        executor.execute();
        return executor;
    }

    /**
     * Bind the context to current scope, exchange the context if it's already existed
     * Sample usage:
     * try (var guard = context.bindScope()) {
     * ......
     * }
     */
    public ScopeGuard bindScope() {
        return ScopeGuard.bind(this);
    }

    // Change current catalog of this session, and reset current database.
    // We can support "use 'catalog <catalog_name>'" from mysql client or "use catalog <catalog_name>" from jdbc.
    public void changeCatalog(String newCatalogName) throws DdlException {
        sessionEnvironmentContext.changeCatalog(newCatalogName);
    }

    // Change current catalog and database of this session.
    // identifier could be "CATALOG.DB" or "DB".
    // For "CATALOG.DB", we change the current catalog database.
    // For "DB", we keep the current catalog and change the current database.
    public void changeCatalogDb(String identifier) throws DdlException {
        sessionEnvironmentContext.changeCatalogDb(identifier);
    }

    public boolean isLeaderTransferred() {
        return sessionExecutionContext.isLeaderTransferred();
    }

    public void setIsLeaderTransferred(boolean isLeaderTransferred) {
        sessionExecutionContext.setLeaderTransferred(isLeaderTransferred);
    }

    /**
     * Set thread-local context for the scope, and remove it after leaving the scope
     */
    public static class ScopeGuard implements AutoCloseable {

        private boolean set = false;
        private ConnectContext prev;

        private ScopeGuard() {
        }

        private static ScopeGuard bind(ConnectContext session) {
            ScopeGuard res = new ScopeGuard();
            res.prev = exchangeThreadLocalInfo(session);
            res.set = true;
            return res;
        }

        public ConnectContext prev() {
            return prev;
        }

        @Override
        public void close() {
            if (set) {
                ConnectContext.remove();
            }
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    public class ThreadInfo {
        public boolean isRunning() {
            return getState().isRunning();
        }

        public List<String> toRow(long nowMs, boolean full) {
            List<String> row = Lists.newArrayList();
            row.add("" + getConnectionId());
            row.add(ClusterNamespace.getNameFromFullName(getQualifiedUser()));
            // Ip + port
            if (ConnectContext.this instanceof HttpConnectContext) {
                String remoteAddress = ((HttpConnectContext) (ConnectContext.this)).getRemoteAddress();
                row.add(remoteAddress);
            } else {
                row.add(getMysqlChannel().getRemoteHostPortString());
            }
            row.add(ClusterNamespace.getNameFromFullName(getDatabase()));
            // Command
            row.add(getCommand().toString());
            // connection start Time
            row.add(TimeUtils.longToTimeString(getConnectionStartTime()));
            // Time
            row.add("" + (nowMs - getStartTime()) / 1000);
            // State
            row.add(getState().toString());
            // Info
            String stmt = "";
            if (getExecutor() != null) {
                stmt = getExecutor().getOriginStmtInString();
                // refers to https://mariadb.com/kb/en/show-processlist/
                // `show full processlist` will output full SQL
                // and `show processlist` will output at most 100 chars.
                if (!full && stmt.length() > 100) {
                    stmt = stmt.substring(0, 100);
                }
            }
            row.add(stmt);
            if (isForward()) {
                // if query is forward to leader, we can't know its accurate status in query queue,
                // so isPending should not be displayed
                row.add("");
            } else {
                row.add(Boolean.toString(isPending()));
            }
            // warehouse
            row.add(getSessionVariable().getWarehouseName());
            // cngroup
            row.add(getCurrentComputeResourceName());
            return row;
        }
    }

    public boolean isIdleLastFor(long milliSeconds) {
        if (getCommand() != MysqlCommand.COM_SLEEP) {
            return false;
        }

        return getEndTimeInstant().isAfter(getStartTimeInstant())
                && getEndTimeInstant().plusMillis(milliSeconds).isBefore(Instant.now());
    }

    public long getCurrentThreadAllocatedMemory() {
        return sessionExecutionContext.getCurrentThreadAllocatedMemory();
    }

    public void setCurrentThreadAllocatedMemory(long currentThreadAllocatedMemory) {
        sessionExecutionContext.setCurrentThreadAllocatedMemory(currentThreadAllocatedMemory);
    }

    public long getCurrentThreadId() {
        return sessionExecutionContext.getCurrentThreadId();
    }

    public void setCurrentThreadId(long currentThreadId) {
        sessionExecutionContext.setCurrentThreadId(currentThreadId);
    }

    public interface Listener {
        /**
         * Trigger when query is finished
         */
        void onQueryFinished(ConnectContext state);
    }

    public void registerListener(Listener listener) {
        this.sessionExecutionContext.registerListener(listener);
    }

    public void onQueryFinished() {
        for (Listener listener : sessionExecutionContext.getListeners()) {
            try {
                listener.onQueryFinished(this);
            } catch (Exception e) {
                // ignore
                LOG.warn("onQueryFinished error", e);
            }
        }

        try {
            sessionExecutionContext.getAuditEventBuilder().setCNGroup(getCurrentComputeResourceName());
        } catch (Exception e) {
            LOG.warn("set cn group name failed", e);
        }
    }
}

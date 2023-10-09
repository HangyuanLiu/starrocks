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
package com.starrocks.authentication;

import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.journal.kv.BDBKVClient;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.UserPrivilegeCollectionV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthenticationMgrKV {
    private static final Logger LOG = LogManager.getLogger(AuthenticationMgr.class);

    static public void createUser(CreateUserStmt stmt) {
        UserIdentity userIdentity = stmt.getUserIdentity();
        UserAuthenticationInfo info = stmt.getAuthenticationInfo();

        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        try {
            if (BDBKVClient.contains(transaction, userIdentity)) {
                // Existence verification has been performed in the Analyzer stage. If it exists here,
                // it may be that other threads have performed the same operation, and return directly here
                LOG.info("Operation CREATE USER failed for " + stmt.getUserIdentity()
                        + " : user " + stmt.getUserIdentity() + " already exists");
                return;
            }

            OperationStatus status = BDBKVClient.putNoOverwrite(transaction, "authentication", userIdentity, info);
            if (!status.equals(OperationStatus.SUCCESS)) {
                throw new Exception();
            }

            if (BDBKVClient.contains(transaction, userIdentity.getQualifiedUser())) {
                BDBKVClient.putNoOverwrite(transaction, "user_property", userIdentity.getQualifiedUser(), new UserProperty());
            }

            AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            //UserPrivilegeCollectionV2 collection = authorizationManager.onCreateUser(userIdentity, stmt.getDefaultRoles());

            UserPrivilegeCollectionV2 userPrivilegeCollection = new UserPrivilegeCollectionV2();
            List<String> defaultRoleName = stmt.getDefaultRoles();
            if (!defaultRoleName.isEmpty()) {
                Set<Long> roleIds = new HashSet<>();
                for (String role : defaultRoleName) {
                    Long roleId = authorizationManager.getRoleIdByNameAllowNull(role);
                    userPrivilegeCollection.grantRole(roleId);
                    roleIds.add(roleId);
                }
                userPrivilegeCollection.setDefaultRoleIds(roleIds);
            }
            BDBKVClient.putNoOverwrite(transaction, "authorization", userIdentity, userPrivilegeCollection);

            CreateUserInfo createUserInfo = new CreateUserInfo(userIdentity, info, new UserProperty(), userPrivilegeCollection,
                    authorizationManager.getProviderPluginId(), authorizationManager.getProviderPluginVersion());
            GlobalStateMgr.getCurrentState().getEditLog().log(transaction, OperationType.OP_CREATE_USER_V2, createUserInfo);
            transaction.commit();
        } catch (Exception e) {
            transaction.abort();
        }
    }

    public void dropUser(DropUserStmt stmt) throws DdlException, IOException {
        UserIdentity userIdentity = stmt.getUserIdentity();
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        BDBKVClient.delete(transaction, "authentication", userIdentity);
        BDBKVClient.delete(transaction, "authorization", userIdentity);

        GlobalStateMgr.getCurrentState().getEditLog().log(transaction, OperationType.OP_DROP_USER_V3, userIdentity);
        transaction.commit();
    }

    public void alterUser(UserIdentity userIdentity, UserAuthenticationInfo userAuthenticationInfo) throws DdlException, IOException {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();

        if (!BDBKVClient.contains(transaction, userIdentity)) {
            // Existence verification has been performed in the Analyzer stage. If it not exists here,
            // it may be that other threads have performed the same operation, and return directly here
            LOG.info("Operation ALTER USER failed for " + userIdentity + " : user " + userIdentity + " not exists");
            return;
        }

        BDBKVClient.putNoOverwrite(transaction, "authentication", userIdentity, userAuthenticationInfo);
        GlobalStateMgr.getCurrentState().getEditLog().log(transaction, OperationType.OP_ALTER_USER_V2,
                new AlterUserInfo(userIdentity, userAuthenticationInfo));

        transaction.commit();
    }

    public UserAuthenticationInfo getUserAuthenticationInfoByUserIdentity(UserIdentity userIdentity) {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        String json = BDBKVClient.get(transaction, GsonUtils.GSON.toJson(userIdentity));
        transaction.commit();
        return GsonUtils.GSON.fromJson(json, UserAuthenticationInfo.class);
    }

    public Map<UserIdentity, UserAuthenticationInfo> getUserToAuthenticationInfo() {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();

        Map<String, String> m = BDBKVClient.getPrefix(transaction, "authentication/");

        transaction.commit();

        Map<UserIdentity, UserAuthenticationInfo> userToAuthenticationInfo = new HashMap<>();
        for (Map.Entry<String, String> entry : m.entrySet()) {
            UserIdentity userIdentity = GsonUtils.GSON.fromJson(entry.getKey(), UserIdentity.class);
            UserAuthenticationInfo userAuthenticationInfo =
                    GsonUtils.GSON.fromJson(entry.getValue(), UserAuthenticationInfo.class);
            userToAuthenticationInfo.put(userIdentity, userAuthenticationInfo);
        }

        return userToAuthenticationInfo;
    }

    public boolean doesUserExist(UserIdentity userIdentity) {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        boolean result = BDBKVClient.contains(transaction, userIdentity);
        transaction.commit();
        return result;
    }

    public UserIdentity checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString) {
        String[] authChain = Config.authentication_chain;
        UserIdentity authenticatedUser = null;
        for (String authMechanism : authChain) {
            if (authenticatedUser != null) {
                break;
            }

            if (authMechanism.equals(ConfigBase.AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                authenticatedUser = checkPasswordForNative(remoteUser, remoteHost, remotePasswd, randomString);
            } else {
                authenticatedUser = checkPasswordForNonNative(
                        remoteUser, remoteHost, remotePasswd, randomString, authMechanism);
            }
        }

        return authenticatedUser;
    }

    private UserIdentity checkPasswordForNative(
            String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString) {
        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                getBestMatchedUserIdentity(remoteUser, remoteHost);
        if (matchedUserIdentity == null) {
            LOG.debug("cannot find user {}@{}", remoteUser, remoteHost);
        } else {
            try {
                AuthenticationProvider provider =
                        AuthenticationProviderFactory.create(matchedUserIdentity.getValue().getAuthPlugin());
                provider.authenticate(remoteUser, remoteHost, remotePasswd, randomString,
                        matchedUserIdentity.getValue());
                return matchedUserIdentity.getKey();
            } catch (AuthenticationException e) {
                LOG.debug("failed to authenticate for native, user: {}@{}, error: {}",
                        remoteUser, remoteHost, e.getMessage());
            }
        }

        return null;
    }

    public Map.Entry<UserIdentity, UserAuthenticationInfo> getBestMatchedUserIdentity(String remoteUser, String remoteHost) {
        Map<UserIdentity, UserAuthenticationInfo> getUserToAuthenticationInfo = getUserToAuthenticationInfo();

        return getUserToAuthenticationInfo.entrySet().stream()
                .filter(entry -> match(remoteUser, remoteHost, entry.getKey().isDomain(), entry.getValue()))
                .findFirst().orElse(null);
    }

    // resolve hostname to ip
    private Map<String, Set<String>> hostnameToIpSet = new HashMap<>();
    private final ReentrantReadWriteLock hostnameToIpLock = new ReentrantReadWriteLock();

    private boolean match(String remoteUser, String remoteHost, boolean isDomain, UserAuthenticationInfo info) {
        // quickly filter unmatched entries by username
        if (!info.matchUser(remoteUser)) {
            return false;
        }
        if (isDomain) {
            // check for resolved ips
            this.hostnameToIpLock.readLock().lock();
            try {
                Set<String> ipSet = hostnameToIpSet.get(info.getOrigHost());
                if (ipSet == null) {
                    return false;
                }
                return ipSet.contains(remoteHost);
            } finally {
                this.hostnameToIpLock.readLock().unlock();
            }
        } else {
            return info.matchHost(remoteHost);
        }
    }

    /**
     * called by DomainResolver to periodically update hostname -> ip set
     */
    public void setHostnameToIpSet(Map<String, Set<String>> hostnameToIpSet) {
        this.hostnameToIpLock.writeLock().lock();
        try {
            this.hostnameToIpSet = hostnameToIpSet;
        } finally {
            this.hostnameToIpLock.writeLock().unlock();
        }
    }

    // UserProperty
    public long getMaxConn(String userName) {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        BDBKVClient.get(transaction, "authentication/user_property/" + userName);


        UserProperty userProperty = userNameToProperty.get(userName);
        if (userProperty == null) {
            // TODO(yiming): find a better way to specify max connections for external user, like ldap, kerberos etc.
            return AuthenticationMgr.DEFAULT_MAX_CONNECTION_FOR_EXTERNAL_USER;
        } else {
            return userNameToProperty.get(userName).getMaxConn();
        }
    }
}

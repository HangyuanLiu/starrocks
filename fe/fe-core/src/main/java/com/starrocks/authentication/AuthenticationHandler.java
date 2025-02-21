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

import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthenticationHandler {
    private static final Logger LOG = LogManager.getLogger(AuthenticationHandler.class);

    // scramble: data receive from server.
    // randomString: data send by server in plug-in data field
    // user_name#HIGH@cluster_name
    public static boolean authenticate(ConnectContext context, String remoteUser, byte[] remotePasswd, byte[] randomString) {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String remoteHost = context.getMysqlChannel().getRemoteIp();

        UserIdentity authenticatedUser = null;
        String groupProviderName = null;
        List<String> authenticatedGroupList = null;

        if (!Config.enable_auth_check) {
            Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                    authenticationMgr.getBestMatchedUserIdentity(remoteUser, remoteHost);
            if (matchedUserIdentity == null) {
                LOG.info("enable_auth_check is false, but cannot find user '{}'@'{}'", remoteUser, remoteHost);
                ErrorReport.report(ErrorCode.ERR_AUTHENTICATION_FAIL, remoteUser, remotePasswd);
                //throw new AuthenticationException("");
                return false;
            } else {
                authenticatedUser = matchedUserIdentity.getKey();
                groupProviderName = Config.group_provider;
                authenticatedGroupList = List.of(Config.authenticated_group_list);
            }
        } else {
            String[] authChain = Config.authentication_chain;

            for (String authMechanism : authChain) {
                if (authenticatedUser != null) {
                    break;
                }

                if (authMechanism.equals(ConfigBase.AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                    authenticatedUser = checkPasswordForNative(remoteUser, remoteHost, remotePasswd, randomString);
                    groupProviderName = Config.group_provider;
                    authenticatedGroupList = List.of(Config.authenticated_group_list);
                } else {
                    SecurityIntegration securityIntegration = authenticationMgr.getSecurityIntegration(authMechanism);
                    if (securityIntegration == null) {
                        continue;
                    }

                    authenticatedUser = checkPasswordForNonNative(
                            remoteUser, remoteHost, remotePasswd, randomString, securityIntegration);
                    groupProviderName = securityIntegration.getGroupProviderName();
                    if (groupProviderName == null) {
                        groupProviderName = Config.group_provider;
                    }

                    authenticatedGroupList = securityIntegration.getAuthenticatedGroupList();
                }
            }
        }

        if (authenticatedUser == null) {
            //ErrorReport.report(ErrorCode.ERR_AUTHENTICATION_FAIL, authenticatedUser, remotePasswd);
            //throw new AuthenticationException("");
            ErrorReport.report(ErrorCode.ERR_GROUP_ACCESS_DENY);
            return false;
        }

        context.setCurrentUserIdentity(authenticatedUser);
        if (!authenticatedUser.isEphemeral()) {
            context.setCurrentRoleIds(authenticatedUser);
            context.setAuthDataSalt(randomString);
        }
        context.setQualifiedUser(remoteUser);

        Set<String> groups = getGroups(authenticatedUser, groupProviderName);
        context.setGroups(groups);

        if (!authenticatedGroupList.isEmpty()) {
            Set<String> intersection = new HashSet<>(groups);
            intersection.retainAll(authenticatedGroupList);
            if (intersection.isEmpty()) {
                ErrorReport.report(ErrorCode.ERR_GROUP_ACCESS_DENY);
                return false;
            }
        }

        return true;
    }

    private static UserIdentity checkPasswordForNative(
            String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString) {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                authenticationMgr.getBestMatchedUserIdentity(remoteUser, remoteHost);

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

    protected static UserIdentity checkPasswordForNonNative(
            String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
            SecurityIntegration securityIntegration) {

        if (securityIntegration == null) {
            //LOG.info("'{}' authentication mechanism not found", authMechanism);
        } else {
            try {
                AuthenticationProvider provider = securityIntegration.getAuthenticationProvider();
                UserAuthenticationInfo userAuthenticationInfo = new UserAuthenticationInfo();
                userAuthenticationInfo.extraInfo.put(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name(),
                        securityIntegration);
                provider.authenticate(remoteUser, remoteHost, remotePasswd, randomString,
                        userAuthenticationInfo);
                // the ephemeral user is identified as 'username'@'auth_mechanism'
                UserIdentity authenticatedUser = UserIdentity.createEphemeralUserIdent(remoteUser, securityIntegration.getName());
                /*
                ConnectContext currentContext = ConnectContext.get();
                if (currentContext != null) {
                    currentContext.setCurrentRoleIds(new HashSet<>(
                            Collections.singletonList(PrivilegeBuiltinConstants.ROOT_ROLE_ID)));
                }
                 */
                return authenticatedUser;
            } catch (AuthenticationException e) {
                LOG.debug("failed to authenticate, user: {}@{}, security integration: {}, error: {}",
                        remoteUser, remoteHost, securityIntegration, e.getMessage());
            }
        }

        return null;
    }

    public static Set<String> getGroups(UserIdentity userIdentity, String groupProviderName) {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        GroupProvider groupProvider = authenticationMgr.getGroupProvider(groupProviderName);
        if (groupProvider == null) {
            return new HashSet<>();
        }
        return groupProvider.getGroup(userIdentity);
    }
}

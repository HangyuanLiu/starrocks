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

package com.starrocks.authorization;

import java.util.HashSet;
import java.util.Set;

/**
 * AuthorizationContext encapsulates authorization-related session state such as
 * currently effective role ids and group memberships.
 */
public class AuthorizationContext {
    // currentRoleIds is the role that has taken effect in the current session.
    private Set<Long> currentRoleIds = new HashSet<>();

    // groups of current user
    private Set<String> groups = new HashSet<>();

    // Bypass the authorizer check for certain cases
    private boolean bypassAuthorizerCheck = false;

    public AuthorizationContext() {
    }

    public Set<Long> getCurrentRoleIds() {
        return currentRoleIds;
    }

    public void setCurrentRoleIds(Set<Long> currentRoleIds) {
        this.currentRoleIds = currentRoleIds;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public boolean isBypassAuthorizerCheck() {
        return bypassAuthorizerCheck;
    }

    public void setBypassAuthorizerCheck(boolean bypassAuthorizerCheck) {
        this.bypassAuthorizerCheck = bypassAuthorizerCheck;
    }
}



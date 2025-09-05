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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Statement for adding backends to blacklist.
 */
public final class AddBackendBlackListStmt extends StatementBase {
    /**
     * The list of backend IDs to add to blacklist.
     */
    private final List<Long> backendIds;

    /**
     * Constructs an AddBackendBlackListStmt.
     *
     * @param ids the list of backend IDs to add to blacklist
     * @param pos the position in the source code
     */
    public AddBackendBlackListStmt(final List<Long> ids, final NodePosition pos) {
        super(pos);
        this.backendIds = ids;
    }

    /**
     * Gets the list of backend IDs to add to blacklist.
     *
     * @return the backend IDs
     */
    public List<Long> getBackendIds() {
        return backendIds;
    }

    @Override
    public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
        return visitor.visitAddBackendBlackListStatement(this, context);
    }
}

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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Clause for ALTER MATERIALIZED VIEW mv PARTITION BY <new_expr>.
 * Changes the partition scheme of a materialized view.
 */
public class AlterMVPartitionByClause extends AlterTableClause {
    private final List<Expr> partitionByExprs;

    public AlterMVPartitionByClause(List<Expr> partitionByExprs, NodePosition pos) {
        super(pos);
        this.partitionByExprs = partitionByExprs;
    }

    public List<Expr> getPartitionByExprs() {
        return partitionByExprs;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMVPartitionByClause(this, context);
    }
}

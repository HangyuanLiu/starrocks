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
package com.starrocks.privilege;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PolicyManager {
    @SerializedName(value = "m")
    private final Map<String, Policy> p;

    public PolicyManager() {
        p = new HashMap<>();
    }

    public Policy getSecurityPolicy(String securityPolicyName) {
        return p.get(securityPolicyName);
    }

    static public QueryStatement buildView(Relation relation) {
        //FIXME: MOCK Policy Manger
        PolicyManager policyManager = new PolicyManager();

        TableName tableName = relation.getResolveTableName();

        List<SelectListItem> selectListItemList = new ArrayList<>();
        Expr rewriteExpr = null;
        boolean hasPolicy = false;

        List<Column> columns;
        if (relation instanceof ViewRelation) {
            ViewRelation viewRelation = (ViewRelation) relation;
            columns = viewRelation.getView().getBaseSchema();
        } else {
            TableRelation tableRelation = (TableRelation) relation;
            columns = tableRelation.getTable().getBaseSchema();
        }

        for (Column column : columns) {
            if (column.hasSecurityPolicy()) {
                hasPolicy = true;
                Policy policy = policyManager.getSecurityPolicy(column.getSecurityPolicy());
                if (policy.isColumnPolicy()) {
                    RewriteAliasVisitor r = new RewriteAliasVisitor(policy.slots);
                    Expr project = r.visit(policy.policyPredicate);
                    selectListItemList.add(new SelectListItem(project, column.getName(), NodePosition.ZERO));
                } else if (policy.isRowPolicy()) {
                    RewriteAliasVisitor r = new RewriteAliasVisitor(policy.slots);
                    if (rewriteExpr == null) {
                        rewriteExpr = r.visit(policy.policyPredicate);
                    } else {
                        rewriteExpr = Expr.compoundAnd(Lists.newArrayList(r.visit(policy.policyPredicate), rewriteExpr));
                    }
                }
            } else {
                selectListItemList.add(
                        new SelectListItem(new SlotRef(tableName, column.getName()), column.getName(), NodePosition.ZERO));
            }
        }

        if (!hasPolicy) {
            return null;
        }

        SelectRelation selectRelation = new SelectRelation(
                new SelectList(selectListItemList, false),
                relation,
                rewriteExpr,
                null,
                null
        );
        return new QueryStatement(selectRelation);
    }


    private static class RewriteAliasVisitor extends AstVisitor<Expr, Void> {
        Map<SlotRef, SlotRef> map;

        public RewriteAliasVisitor(Map<SlotRef, SlotRef> map) {
            this.map = map;
        }

        @Override
        public Expr visit(ParseNode expr) {
            return visit(expr, null);
        }

        @Override
        public Expr visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Void context) {
            return map.getOrDefault(slotRef, slotRef);
        }
    }
}

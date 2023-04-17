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
package ee.starrocks.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.privilege.ColumnMaskingPolicyContext;
import com.starrocks.privilege.Policy;
import com.starrocks.privilege.PolicyContext;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.RowAccessPolicyContext;
import com.starrocks.privilege.SecurityPolicyManager;
import com.starrocks.privilege.TablePEntryObject;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecurityPolicyRewriteRule {
    public static QueryStatement buildView(Relation relation) {
        SecurityPolicyManager policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();

        Table table;
        if (relation instanceof TableRelation) {
            table = ((TableRelation) relation).getTable();
        } else {
            return null;
        }

        TableName tableName = relation.getResolveTableName();

        TablePEntryObject tablePEntryObject = null;
        try {
            tablePEntryObject = TablePEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
        } catch (PrivilegeException e) {
            throw new SemanticException("Error");
        }

        List<Column> columns;
        if (relation instanceof ViewRelation) {
            ViewRelation viewRelation = (ViewRelation) relation;
            columns = viewRelation.getView().getBaseSchema();
        } else {
            TableRelation tableRelation = (TableRelation) relation;
            columns = tableRelation.getTable().getBaseSchema();
        }

        List<SelectListItem> selectListItemList = new ArrayList<>();
        Expr rewriteExpr = null;

        if (policyManager.hasTableAppliedPolicy(tablePEntryObject)) {
            PolicyContext tableAppliedPolicyInfo =
                    policyManager.getTableAppliedPolicyInfo(tablePEntryObject);

            if (!tableAppliedPolicyInfo.getRowAccessPolicyApply().isEmpty()) {
                for (RowAccessPolicyContext rowAccessPolicyInfo : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
                    Policy rowAccessPolicy = policyManager.getPolicyById(rowAccessPolicyInfo.getPolicyId());

                    if (!rowAccessPolicyInfo.getOnColumns().isEmpty()) {
                        Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                        List<String> onColumns = rowAccessPolicyInfo.getOnColumns();
                        List<String> argNames = rowAccessPolicy.getArgNames();

                        for (int i = 0; i < rowAccessPolicyInfo.getOnColumns().size(); ++i) {
                            onColumnsMap.put(new SlotRef(null, argNames.get(i)), new SlotRef(tableName, onColumns.get(i)));
                        }

                        RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);
                        if (rewriteExpr == null) {
                            rewriteExpr = r.visit(rowAccessPolicy.getPolicyExpression().clone());
                        } else {
                            rewriteExpr = Expr.compoundAnd(Lists.newArrayList(
                                    r.visit(rowAccessPolicy.getPolicyExpression().clone()), rewriteExpr));
                        }
                    } else {
                        rewriteExpr = rowAccessPolicy.getPolicyExpression();
                    }
                }
            }

            Map<String, ColumnMaskingPolicyContext> maskingPolicyApply = tableAppliedPolicyInfo.getMaskingPolicyApply();
            for (Column column : columns) {
                ColumnMaskingPolicyContext maskingPolicyInfo = maskingPolicyApply.get(column.getName());
                if (maskingPolicyInfo != null) {

                    Policy maskingPolicy = policyManager.getPolicyById(maskingPolicyInfo.getPolicyId());

                    if (!maskingPolicyInfo.getUsingColumns().isEmpty()) {
                        Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                        List<String> usingColumns = maskingPolicyInfo.getUsingColumns();
                        List<String> argNames = maskingPolicy.getArgNames();

                        for (int i = 0; i < maskingPolicyInfo.getUsingColumns().size(); ++i) {
                            onColumnsMap.put(new SlotRef(null, argNames.get(i)), new SlotRef(tableName, usingColumns.get(i)));
                        }

                        RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);
                        Expr project = r.visit(maskingPolicy.getPolicyExpression().clone());
                        selectListItemList.add(new SelectListItem(project, column.getName(), NodePosition.ZERO));
                    }
                } else {
                    selectListItemList.add(
                            new SelectListItem(new SlotRef(tableName, column.getName()), column.getName(), NodePosition.ZERO));
                }
            }

            SelectRelation selectRelation = new SelectRelation(
                    new SelectList(selectListItemList, false),
                    relation,
                    rewriteExpr,
                    null,
                    null);
            selectRelation.setOrderBy(Collections.emptyList());
            return new QueryStatement(selectRelation);
        } else {
            return null;
        }
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

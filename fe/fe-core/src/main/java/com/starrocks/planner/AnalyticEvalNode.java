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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AnalyticEvalNode.java

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

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprExplain;
import com.starrocks.planner.expression.ExecExprSerializer;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TAnalyticNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalAnalyticNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class AnalyticEvalNode extends PlanNode {
    private List<ExecExpr> analyticFnCalls;

    // Partitioning exprs from the AnalyticInfo
    private final List<ExecExpr> partitionExprs;

    // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
    private List<ExecExpr> substitutedPartitionExprs;

    // Order-by expressions and flags (stored separately instead of as OrderByElement
    // because the expressions are ExecExpr, not AST Expr)
    private List<ExecExpr> orderByExprs;
    private List<Boolean> orderByIsAsc;
    private List<Boolean> orderByNullsFirst;

    private final AnalyticWindow analyticWindow;

    private final boolean useHashBasedPartition;
    private final boolean isSkewed;

    // Physical tuples used/produced by this analytic node.
    private final TupleDescriptor intermediateTupleDesc;
    private final TupleDescriptor outputTupleDesc;

    // predicates constructed from partitionExprs_/orderingExprs_ to
    // compare input to buffered tuples
    private final ExecExpr partitionByEq;
    private final ExecExpr orderByEq;
    private final TupleDescriptor bufferedTupleDesc;

    public AnalyticEvalNode(
            PlanNodeId id, PlanNode input, List<ExecExpr> analyticFnCalls,
            List<ExecExpr> partitionExprs,
            List<ExecExpr> orderByExprs, List<Boolean> orderByIsAsc, List<Boolean> orderByNullsFirst,
            AnalyticWindow analyticWindow,
            boolean useHashBasedPartition,
            boolean isSkewed,
            TupleDescriptor intermediateTupleDesc,
            TupleDescriptor outputTupleDesc,
            ExecExpr partitionByEq, ExecExpr orderByEq, TupleDescriptor bufferedTupleDesc) {
        super(id, input.getTupleIds(), "ANALYTIC");
        Preconditions.checkState(!tupleIds.contains(outputTupleDesc.getId()));
        // we're materializing the input row augmented with the analytic output tuple
        tupleIds.add(outputTupleDesc.getId());
        this.analyticFnCalls = analyticFnCalls;
        this.partitionExprs = partitionExprs;
        this.orderByExprs = orderByExprs;
        this.orderByIsAsc = orderByIsAsc;
        this.orderByNullsFirst = orderByNullsFirst;
        this.analyticWindow = analyticWindow;
        this.useHashBasedPartition = useHashBasedPartition;
        this.isSkewed = isSkewed;
        this.intermediateTupleDesc = intermediateTupleDesc;
        this.outputTupleDesc = outputTupleDesc;
        this.partitionByEq = partitionByEq;
        this.orderByEq = orderByEq;
        this.bufferedTupleDesc = bufferedTupleDesc;
        children.add(input);
        nullableTupleIds = Sets.newHashSet(input.getNullableTupleIds());
    }

    public List<ExecExpr> getAnalyticFnCalls() {
        return analyticFnCalls;
    }

    public List<ExecExpr> getPartitionExprs() {
        return partitionExprs;
    }

    public List<ExecExpr> getOrderByExprs() {
        return orderByExprs;
    }

    public List<Boolean> getOrderByIsAsc() {
        return orderByIsAsc;
    }

    public List<Boolean> getOrderByNullsFirst() {
        return orderByNullsFirst;
    }

    @Override
    protected void computeStats() {
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this)
                .add("analyticFnCalls", ExecExprExplain.explainList(analyticFnCalls))
                .add("partitionExprs", ExecExprExplain.explainList(partitionExprs))
                .add("substitutedPartitionExprs", substitutedPartitionExprs != null ?
                        ExecExprExplain.explainList(substitutedPartitionExprs) : "null")
                .add("orderByExprs", ExecExprExplain.explainList(orderByExprs))
                .add("window", analyticWindow)
                .add("useHashBasedPartition", useHashBasedPartition)
                .add("isSkewed", isSkewed)
                .add("intermediateTid", intermediateTupleDesc != null ? intermediateTupleDesc.getId() : "null")
                .add("outputTid", outputTupleDesc.getId())
                .add("partitionByEq",
                        partitionByEq != null ? ExecExprExplain.explain(partitionByEq) : "null")
                .add("orderByEq",
                        orderByEq != null ? ExecExprExplain.explain(orderByEq) : "null")
                .addValue(super.debugString())
                .toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
        msg.analytic_node = new TAnalyticNode();
        if (intermediateTupleDesc != null) {
            msg.analytic_node.setIntermediate_tuple_id(intermediateTupleDesc.getId().asInt());
        }
        msg.analytic_node.setOutput_tuple_id(outputTupleDesc.getId().asInt());
        msg.analytic_node.setPartition_exprs(ExecExprSerializer.serializeList(substitutedPartitionExprs));
        StringBuilder sqlPartitionKeysBuilder = new StringBuilder();
        for (ExecExpr e : substitutedPartitionExprs) {
            if (sqlPartitionKeysBuilder.length() > 0) {
                sqlPartitionKeysBuilder.append(", ");
            }
            sqlPartitionKeysBuilder.append(ExecExprExplain.explain(e));
        }
        if (sqlPartitionKeysBuilder.length() > 0) {
            msg.analytic_node.setSql_partition_keys(sqlPartitionKeysBuilder.toString());
        }
        msg.analytic_node.setOrder_by_exprs(ExecExprSerializer.serializeList(orderByExprs));
        msg.analytic_node.setAnalytic_functions(ExecExprSerializer.serializeList(analyticFnCalls));
        StringBuilder sqlAggFuncBuilder = new StringBuilder();
        for (ExecExpr e : analyticFnCalls) {
            if (sqlAggFuncBuilder.length() > 0) {
                sqlAggFuncBuilder.append(", ");
            }
            sqlAggFuncBuilder.append(ExecExprExplain.explain(e));
        }
        if (sqlAggFuncBuilder.length() > 0) {
            msg.analytic_node.setSql_aggregate_functions(sqlAggFuncBuilder.toString());
        }

        if (analyticWindow == null) {
            if (!orderByExprs.isEmpty()) {
                msg.analytic_node.setWindow(
                        ExprToThrift.analyticWindowToThrift(AnalyticWindow.DEFAULT_WINDOW));
            }
        } else {
            // TODO: Window boundaries should have range_offset_predicate set
            msg.analytic_node.setWindow(ExprToThrift.analyticWindowToThrift(analyticWindow));
        }

        if (partitionByEq != null) {
            msg.analytic_node.setPartition_by_eq(ExecExprSerializer.serialize(partitionByEq));
        }

        if (orderByEq != null) {
            msg.analytic_node.setOrder_by_eq(ExecExprSerializer.serialize(orderByEq));
        }

        msg.analytic_node.setUse_hash_based_partition(useHashBasedPartition);
        msg.analytic_node.setIs_skewed(isSkewed);

        if (bufferedTupleDesc != null) {
            msg.analytic_node.setBuffered_tuple_id(bufferedTupleDesc.getId().asInt());
        }
        msg.analytic_node.setHas_outer_join_child(hasNullableGenerateChild);
    }

    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        boolean verbose = TExplainLevel.VERBOSE.equals(detailLevel) || TExplainLevel.COSTS.equals(detailLevel);
        output.append(prefix).append("functions: ");
        List<String> strings = Lists.newArrayList();

        for (ExecExpr fnCall : analyticFnCalls) {
            strings.add("[");
            strings.add(verbose ? ExecExprExplain.verboseExplain(fnCall) : ExecExprExplain.explain(fnCall));
            strings.add("]");
        }

        output.append(Joiner.on(", ").join(strings));
        output.append("\n");

        if (!partitionExprs.isEmpty()) {
            output.append(prefix).append("partition by: ");
            strings.clear();

            for (ExecExpr partitionExpr : partitionExprs) {
                strings.add(verbose ? ExecExprExplain.verboseExplain(partitionExpr) : ExecExprExplain.explain(partitionExpr));
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (!orderByExprs.isEmpty()) {
            output.append(prefix).append("order by: ");
            strings.clear();

            for (int i = 0; i < orderByExprs.size(); i++) {
                StringBuilder element = new StringBuilder();
                element.append(verbose ? ExecExprExplain.verboseExplain(orderByExprs.get(i)) : ExecExprExplain.explain(orderByExprs.get(i)));
                boolean isAsc = orderByIsAsc.get(i);
                boolean nullsFirst = orderByNullsFirst.get(i);
                element.append(isAsc ? " ASC" : " DESC");
                // Only show NULLS FIRST/LAST when non-default
                // StarRocks default: ASC → NULLS FIRST, DESC → NULLS LAST
                if (isAsc && !nullsFirst) {
                    element.append(" NULLS LAST");
                } else if (!isAsc && nullsFirst) {
                    element.append(" NULLS FIRST");
                }
                strings.add(element.toString());
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (analyticWindow != null) {
            output.append(prefix).append("window: ");
            output.append(com.starrocks.sql.ast.expression.ExprToSql.toSql(analyticWindow));
            output.append("\n");
        }

        if (useHashBasedPartition) {
            output.append(prefix).append("useHashBasedPartition").append("\n");
        }
        if (isSkewed) {
            output.append(prefix).append("isSkewed").append("\n");
        }

        return output.toString();
    }

    public void setSubstitutedPartitionExprs(List<ExecExpr> substitutedPartitionExprs) {
        this.substitutedPartitionExprs = substitutedPartitionExprs;
    }

    @Override
    public Optional<List<ExecExpr>> candidatesOfSlotExpr(ExecExpr expr, Function<ExecExpr, Boolean> couldBound) {
        if (!couldBound.apply(expr)) {
            return Optional.empty();
        }
        if (!(expr instanceof ExecSlotRef)) {
            return Optional.empty();
        }
        List<ExecExpr> newSlotExprs = Lists.newArrayList();
        for (ExecExpr pExpr : partitionExprs) {
            // push down only when both of them are slot ref and slot id match.
            if ((pExpr instanceof ExecSlotRef) &&
                    (((ExecSlotRef) pExpr).getSlotId().asInt() == ((ExecSlotRef) expr).getSlotId().asInt())) {
                newSlotExprs.add(pExpr);
            }
        }
        return newSlotExprs.size() > 0 ? Optional.of(newSlotExprs) : Optional.empty();
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterPushDownContext context, ExecExpr probeExpr,
                                          List<ExecExpr> partitionByExprs) {
        RuntimeFilterDescription description = context.getDescription();
        DescriptorTable descTbl = context.getDescTbl();
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!couldBound(probeExpr, description, descTbl)) {
            return false;
        }

        return pushdownRuntimeFilterForChildOrAccept(context, probeExpr,
                candidatesOfSlotExpr(probeExpr, couldBound(description, descTbl)),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs, couldBoundForPartitionExpr()), 0, true);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        List<ExecExpr> conjuncts = normalizer.getConjunctsByPlanNodeId(this);
        normalizer.filterOutPartColRangePredicates(getId(), conjuncts,
                FragmentNormalizer.getExecExprSlotIdSet(partitionExprs));
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalAnalyticNode analyticNode = new TNormalAnalyticNode();
        analyticNode.setPartition_exprs(normalizer.normalizeOrderedExecExprs(substitutedPartitionExprs));
        analyticNode.setOrder_by_exprs(normalizer.normalizeOrderedExecExprs(orderByExprs));
        analyticNode.setAnalytic_functions(normalizer.normalizeExecExprs(analyticFnCalls));
        if (analyticWindow != null) {
            analyticNode.setWindow(ExprToThrift.analyticWindowToThrift(analyticWindow));
        }
        if (intermediateTupleDesc != null) {
            analyticNode.setIntermediate_tuple_id(normalizer.remapTupleId(intermediateTupleDesc.getId()).asInt());
        }
        if (outputTupleDesc != null) {
            analyticNode.setOutput_tuple_id(normalizer.remapTupleId(outputTupleDesc.getId()).asInt());
        }
        if (bufferedTupleDesc != null) {
            analyticNode.setBuffered_tuple_id(normalizer.remapTupleId(bufferedTupleDesc.getId()).asInt());
        }
        if (partitionByEq != null) {
            analyticNode.setPartition_by_eq(normalizer.normalizeExecExpr(partitionByEq));
        }
        if (orderByEq != null) {
            analyticNode.setOrder_by_eq(normalizer.normalizeExecExpr(orderByEq));
        }
        analyticNode.setHas_outer_join_child(hasNullableGenerateChild);
        planNode.setAnalytic_node(analyticNode);
        planNode.setNode_type(TPlanNodeType.ANALYTIC_EVAL_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }
}

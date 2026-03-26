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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/SetOperationNode.java

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprExplain;
import com.starrocks.planner.expression.ExecExprSerializer;
import com.starrocks.planner.expression.ExecExprUtils;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.thrift.TExceptNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TIntersectNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalSetOperationNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TUnionNode;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Node that merges the results of its child plans, Normally, this is done by
 * materializing the corresponding result exprs into a new tuple. However, if
 * a child has an identical tuple layout as the output of the set operation node, and
 * the child only has naked SlotRefs as result exprs, then the child is marked
 * as 'passthrough'. The rows of passthrough children are directly returned by
 * the set operation node, instead of materializing the child's result exprs into new
 * tuples.
 */
public abstract class SetOperationNode extends PlanNode {
    private static final Logger LOG = LoggerFactory.getLogger(SetOperationNode.class);

    // List of set operation result exprs of the originating SetOperationStmt. Used for
    // determining passthrough-compatibility of children.
    protected List<ExecExpr> setOpResultExprs_;

    // Expr lists corresponding to the input query stmts.
    // The ith resultExprList belongs to the ith child.
    // All exprs are resolved to base tables.
    protected List<List<ExecExpr>> resultExprLists_ = Lists.newArrayList();

    // Expr lists that originate from constant select stmts.
    // We keep them separate from the regular expr lists to avoid null children.
    protected List<List<ExecExpr>> constExprLists_ = Lists.newArrayList();

    // Materialized result/const exprs corresponding to materialized slots.
    // Set in init() and substituted against the corresponding child's output smap.
    protected List<List<ExecExpr>> materializedResultExprLists_ = Lists.newArrayList();
    protected List<List<ExecExpr>> materializedConstExprLists_ = Lists.newArrayList();
    protected List<ExecExpr> setOperationOutputList = Lists.newArrayList();

    // Indicates if this UnionNode is inside a subplan.
    protected boolean isInSubplan_;

    // Index of the first non-passthrough child.
    protected int firstMaterializedChildIdx_;

    protected final TupleId tupleId_;

    protected List<Map<Integer, Integer>> outputSlotIdToChildSlotIdMaps = Lists.newArrayList();

    protected List<List<ExecExpr>> localPartitionByExprsList = Lists.newArrayList();
    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName) {
        super(id, tupleId.asList(), planNodeName);
        setOpResultExprs_ = Lists.newArrayList();
        tupleId_ = tupleId;
        isInSubplan_ = false;
    }

    protected SetOperationNode(PlanNodeId id, TupleId tupleId, String planNodeName,
                               List<ExecExpr> setOpResultExprs,
                               boolean isInSubplan) {
        super(id, tupleId.asList(), planNodeName);
        setOpResultExprs_ = setOpResultExprs;
        tupleId_ = tupleId;
        isInSubplan_ = isInSubplan;
    }

    public void addConstExprList(List<ExecExpr> exprs) {
        constExprLists_.add(exprs);
    }

    /**
     * Add a child tree plus its corresponding unresolved resultExprs.
     */
    public void addChild(PlanNode node, List<ExecExpr> resultExprs) {
        super.addChild(node);
        resultExprLists_.add(resultExprs);
    }

    public void setMaterializedResultExprLists_(List<List<ExecExpr>> materializedResultExprLists_) {
        this.materializedResultExprLists_ = materializedResultExprLists_;
    }

    public void setMaterializedConstExprLists_(List<List<ExecExpr>> materializedConstExprLists_) {
        this.materializedConstExprLists_ = materializedConstExprLists_;
    }

    public void setFirstMaterializedChildIdx_(int firstMaterializedChildIdx_) {
        this.firstMaterializedChildIdx_ = firstMaterializedChildIdx_;
    }

    public void setOutputSlotIdToChildSlotIdMaps(List<Map<Integer, Integer>> outputSlotIdToChildSlotIdMaps) {
        this.outputSlotIdToChildSlotIdMaps = outputSlotIdToChildSlotIdMaps;
    }

    public void setLocalPartitionByExprsList(List<List<ExecExpr>> localPartitionByExprsList) {
        this.localPartitionByExprsList = localPartitionByExprsList;
    }

    public void setSetOperationOutputList(List<ExecExpr> setOperationOutputList) {
        this.setOperationOutputList = setOperationOutputList;
    }

    @Override
    public void computeStats() {
    }

    protected void toThrift(TPlanNode msg, TPlanNodeType nodeType) {
        Preconditions.checkState(materializedResultExprLists_.size() == children.size());
        List<List<TExpr>> texprLists = Lists.newArrayList();

        for (List<ExecExpr> exprList : materializedResultExprLists_) {
            texprLists.add(ExecExprSerializer.serializeList(exprList));
        }

        List<List<TExpr>> constTexprLists = Lists.newArrayList();
        for (List<ExecExpr> constTexprList : materializedConstExprLists_) {
            constTexprLists.add(ExecExprSerializer.serializeList(constTexprList));
        }

        List<List<TExpr>> tlocalPartitionByExprsList = Lists.newArrayList();
        for (List<ExecExpr> localPartitionByExprs : localPartitionByExprsList) {
            tlocalPartitionByExprsList.add(ExecExprSerializer.serializeList(localPartitionByExprs));
        }

        Preconditions.checkState(firstMaterializedChildIdx_ <= children.size());
        switch (nodeType) {
            case UNION_NODE:
                msg.union_node = new TUnionNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.union_node.setPass_through_slot_maps(outputSlotIdToChildSlotIdMaps);
                msg.node_type = TPlanNodeType.UNION_NODE;
                if (!tlocalPartitionByExprsList.isEmpty()) {
                    msg.union_node.setLocal_partition_by_exprs(tlocalPartitionByExprsList);
                }
                break;
            case INTERSECT_NODE:
                msg.intersect_node = new TIntersectNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.INTERSECT_NODE;
                if (!tlocalPartitionByExprsList.isEmpty()) {
                    msg.intersect_node.setLocal_partition_by_exprs(tlocalPartitionByExprsList);
                }
                break;
            case EXCEPT_NODE:
                msg.except_node = new TExceptNode(
                        tupleId_.asInt(), texprLists, constTexprLists, firstMaterializedChildIdx_);
                msg.node_type = TPlanNodeType.EXCEPT_NODE;
                if (!tlocalPartitionByExprsList.isEmpty()) {
                    msg.except_node.setLocal_partition_by_exprs(tlocalPartitionByExprsList);
                }
                break;
            default:
                LOG.error("Node type: " + nodeType.toString() + " is invalid.");
                break;
        }
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        // A SetOperationNode may have predicates if a union is set operation inside an inline view,
        // and the enclosing select stmt has predicates referring to the inline view.
        if (CollectionUtils.isNotEmpty(conjuncts)) {
            output.append(prefix).append("predicates: ").append(explainExpr(detailLevel, conjuncts)).append("\n");
        }
        if (CollectionUtils.isNotEmpty(constExprLists_)) {
            boolean verbose = TExplainLevel.VERBOSE.equals(detailLevel) || TExplainLevel.COSTS.equals(detailLevel);
            output.append(prefix).append("constant exprs: ").append("\n");
            for (List<ExecExpr> exprs : constExprLists_) {
                output.append(prefix).append("    ").append(exprs.stream()
                        .map(e -> verbose ? ExecExprExplain.verboseExplain(e) : ExecExprExplain.explain(e))
                        .collect(Collectors.joining(" | "))).append("\n");
            }
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(setOperationOutputList)) {
                output.append(prefix).append("output exprs:").append("\n");
                output.append(prefix).append("    ")
                        .append(setOperationOutputList.stream()
                                .map(ExecExprExplain::verboseExplain)
                                .collect(Collectors.joining(" | ")))
                        .append("\n");
            }

            if (CollectionUtils.isNotEmpty(materializedResultExprLists_)) {
                output.append(prefix).append("child exprs:").append("\n");
                for (List<ExecExpr> exprs : materializedResultExprLists_) {
                    output.append(prefix).append("    ")
                            .append(exprs.stream()
                                    .map(ExecExprExplain::verboseExplain)
                                    .collect(Collectors.joining(" | ")))
                            .append("\n");
                }
            }
            List<String> passThroughNodeIds = Lists.newArrayList();
            for (int i = 0; i < firstMaterializedChildIdx_; ++i) {
                passThroughNodeIds.add(children.get(i).getId().toString());
            }
            if (!passThroughNodeIds.isEmpty()) {
                String result = prefix + "pass-through-operands: ";
                if (passThroughNodeIds.size() == children.size()) {
                    output.append(result).append("all\n");
                } else {
                    output.append(result).append(Joiner.on(",").join(passThroughNodeIds)).append("\n");
                }
            }
        }
        return output.toString();
    }

    @Override
    public boolean canDoReplicatedJoin() {
        return false;
    }

    public Optional<List<ExecExpr>> candidatesOfSlotExprForChild(ExecExpr expr, int childIdx) {
        Map<Integer, Set<Integer>> slotExprOutputSlotIdsMap = Maps.newHashMap();
        if (!(expr instanceof ExecSlotRef)) {
            return Optional.empty();
        }
        if (!ExecExprUtils.isBoundByTupleIds(expr, getTupleIds())) {
            return Optional.empty();
        }
        int slotExprSlotId = ((ExecSlotRef) expr).getSlotId().asInt();
        for (Map<Integer, Integer> map : outputSlotIdToChildSlotIdMaps) {
            if (map.containsKey(slotExprSlotId)) {
                slotExprOutputSlotIdsMap.putIfAbsent(slotExprSlotId, Sets.newHashSet());
                slotExprOutputSlotIdsMap.get(slotExprSlotId).add(map.get(slotExprSlotId));
            }
        }
        if (!slotExprOutputSlotIdsMap.containsKey(slotExprSlotId)) {
            return Optional.empty();
        }

        List<ExecExpr> newSlotExprs = Lists.newArrayList();
        Set<Integer> mappedSlotIds = slotExprOutputSlotIdsMap.get(slotExprSlotId);
        // try to push all children if any expr of a child can match `probeExpr`
        for (ExecExpr mexpr : materializedResultExprLists_.get(childIdx)) {
            if ((mexpr instanceof ExecSlotRef) &&
                    mappedSlotIds.contains(((ExecSlotRef) mexpr).getSlotId().asInt())) {
                newSlotExprs.add(mexpr);
            }
        }
        return newSlotExprs.isEmpty() ? Optional.empty() : Optional.of(newSlotExprs);
    }

    public Optional<List<List<ExecExpr>>> candidatesOfSlotExprsForChild(List<ExecExpr> exprs, int childIdx) {
        if (!exprs.stream().allMatch(expr -> candidatesOfSlotExprForChild(expr, childIdx).isPresent())) {
            return Optional.empty();
        }
        List<List<ExecExpr>> candidatesOfSlotExprs =
                exprs.stream().map(expr -> candidatesOfSlotExprForChild(expr, childIdx).get()).collect(Collectors.toList());
        return Optional.of(candidateOfPartitionByExprs(candidatesOfSlotExprs));
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterPushDownContext context, ExecExpr probeExpr,
                                          List<ExecExpr> partitionByExprs) {
        RuntimeFilterDescription description = context.getDescription();
        if (!canPushDownRuntimeFilter()) {
            return false;
        }
        boolean isBound = ExecExprUtils.isBoundByTupleIds(probeExpr, getTupleIds()) &&
                partitionByExprs.stream().allMatch(expr -> ExecExprUtils.isBoundByTupleIds(expr, getTupleIds()));
        if (!isBound) {
            return false;
        }

        if (probeExpr instanceof ExecSlotRef) {
            boolean pushDown = false;
            // try to push all children if any expr of a child can match `probeExpr`
            for (int i = 0; i < materializedResultExprLists_.size(); i++) {
                pushDown |= pushdownRuntimeFilterForChildOrAccept(context, probeExpr,
                        candidatesOfSlotExprForChild(probeExpr, i), partitionByExprs,
                        candidatesOfSlotExprsForChild(partitionByExprs, i), i, false);
            }
            if (pushDown) {
                return true;
            }
        }

        if (description.canProbeUse(this, context)) {
            // can not push down to children.
            // use runtime filter at this level.
            description.addProbeExpr(id.asInt(), probeExpr);
            description.addPartitionByExprsIfNeeded(id.asInt(), probeExpr, partitionByExprs);
            probeRuntimeFilters.add(description);
            return true;
        }
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalSetOperationNode setOperationNode = new TNormalSetOperationNode();
        setOperationNode.setTuple_id(normalizer.remapTupleId(tupleId_).asInt());
        setOperationNode.setResult_expr_lists(
                materializedConstExprLists_.stream().map(normalizer::normalizeOrderedExecExprs)
                        .collect(Collectors.toList()));
        setOperationNode.setConst_expr_lists(
                constExprLists_.stream().map(normalizer::normalizeOrderedExecExprs).collect(Collectors.toList()));
        setOperationNode.setFirst_materialized_child_idx(firstMaterializedChildIdx_);
        if (this instanceof UnionNode) {
            planNode.setNode_type(TPlanNodeType.UNION_NODE);
        } else if (this instanceof ExceptNode) {
            planNode.setNode_type(TPlanNodeType.EXCEPT_NODE);
        } else if (this instanceof IntersectNode) {
            planNode.setNode_type(TPlanNodeType.INTERSECT_NODE);
        } else {
            Preconditions.checkState(false);
        }
        planNode.setSet_operation_node(setOperationNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
        super.toNormalForm(planNode, normalizer);
    }

    @Override
    public void collectEquivRelation(FragmentNormalizer normalizer) {
        this.outputSlotIdToChildSlotIdMaps.forEach(map ->
                map.forEach((k, v) -> normalizer.getEquivRelation().union(new SlotId(k), new SlotId(v))));
    }
}

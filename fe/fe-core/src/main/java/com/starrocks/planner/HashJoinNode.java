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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/HashJoinNode.java

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

import com.starrocks.common.Config;
import com.starrocks.planner.expression.ExecBinaryPredicate;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprExplain;
import com.starrocks.planner.expression.ExecExprSerializer;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.planner.expression.ExprOpcodeRegistry;
import com.starrocks.planner.expression.ThriftEnumConverter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.thrift.TAsofJoinCondition;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.THashJoinNode;
import com.starrocks.thrift.TNormalHashJoinNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class HashJoinNode extends JoinNode {
    private boolean isSkewJoin = false;
    // only set when isSkewJoin = true
    private HashJoinNode skewJoinFriend;

    // only set when isSkewJoin = true && shuffle join
    private Map<Integer, Integer> eqJoinConjunctsIndexToRfId;

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
                        List<ExecExpr> eqJoinConjuncts, List<ExecExpr> otherJoinConjuncts) {
        super("HASH JOIN", id, outer, inner, joinOp, eqJoinConjuncts, otherJoinConjuncts);
    }

    public boolean isSkewJoin() {
        return isSkewJoin;
    }

    public void setSkewJoin(boolean skewJoin) {
        this.isSkewJoin = skewJoin;
    }

    public boolean isSkewShuffleJoin() {
        return isSkewJoin && distrMode == DistributionMode.PARTITIONED;
    }

    public boolean isSkewBroadJoin() {
        return isSkewJoin && distrMode == DistributionMode.BROADCAST;
    }

    public HashJoinNode getSkewJoinFriend() {
        return skewJoinFriend;
    }

    public void setSkewJoinFriend(HashJoinNode skewJoinFriend) {
        this.skewJoinFriend = skewJoinFriend;
    }

    public Map<Integer, Integer> getEqJoinConjunctsIndexToRfId() {
        if (eqJoinConjunctsIndexToRfId == null) {
            eqJoinConjunctsIndexToRfId = new HashMap<>();
        }
        return eqJoinConjunctsIndexToRfId;
    }

    public int getRfIdByEqJoinConjunctsIndex(int index) {
        return eqJoinConjunctsIndexToRfId.get(index);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = ThriftEnumConverter.joinOperatorToThrift(joinOp);
        msg.hash_join_node.distribution_mode = distrMode.toThrift();
        StringBuilder sqlJoinPredicatesBuilder = new StringBuilder();
        for (ExecBinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(
                    ExecExprSerializer.serialize(eqJoinPredicate.getChild(0)),
                    ExecExprSerializer.serialize(eqJoinPredicate.getChild(1)));
            eqJoinCondition.setOpcode(ExprOpcodeRegistry.getBinaryOpcode(eqJoinPredicate.getOp()));
            msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(ExecExprExplain.explain(eqJoinPredicate));
        }

        if (joinOp.isAsofJoin() && asofJoinConjunct != null) {
            TAsofJoinCondition asofJoinCondition = new TAsofJoinCondition(
                    ExecExprSerializer.serialize(asofJoinConjunct.getChild(0)),
                    ExecExprSerializer.serialize(asofJoinConjunct.getChild(1)),
                    ExprOpcodeRegistry.getBinaryOpcode(asofJoinConjunct.getOp()));
            msg.hash_join_node.setAsof_join_condition(asofJoinCondition);
            if (!sqlJoinPredicatesBuilder.isEmpty()) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(ExecExprExplain.explain(asofJoinConjunct));
        }

        for (ExecExpr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOther_join_conjuncts(ExecExprSerializer.serialize(e));
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(ExecExprExplain.explain(e));
        }
        if (sqlJoinPredicatesBuilder.length() > 0) {
            msg.hash_join_node.setSql_join_predicates(sqlJoinPredicatesBuilder.toString());
        }
        if (!conjuncts.isEmpty()) {
            StringBuilder sqlPredicatesBuilder = new StringBuilder();
            for (ExecExpr e : conjuncts) {
                if (sqlPredicatesBuilder.length() > 0) {
                    sqlPredicatesBuilder.append(", ");
                }
                sqlPredicatesBuilder.append(ExecExprExplain.explain(e));
            }
            if (sqlPredicatesBuilder.length() > 0) {
                msg.hash_join_node.setSql_predicates(sqlPredicatesBuilder.toString());
            }
        }
        msg.hash_join_node.setIs_push_down(isPushDown);
        if (!buildRuntimeFilters.isEmpty()) {
            msg.hash_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
        SessionVariable sv = ConnectContext.get().getSessionVariable();

        msg.hash_join_node.setLate_materialization(enableLateMaterialization);
        // predicate filtration rate
        double predicateRate = getCardinality() / (double) getChild(0).getCardinality();
        if (enableLateMaterialization) {
            // If join late materialize is turned on higher filtering can lead to performance degradation.
            if (predicateRate > Config.partition_hash_join_min_cardinality_rate) {
                msg.hash_join_node.setEnable_partition_hash_join(sv.enablePartitionHashJoin());
            } else {
                msg.hash_join_node.setEnable_partition_hash_join(false);
            }
        } else {
            msg.hash_join_node.setEnable_partition_hash_join(sv.enablePartitionHashJoin());
        }
        msg.hash_join_node.setBuild_runtime_filters_from_planner(sv.getEnableGlobalRuntimeFilter());

        if (partitionExprs != null) {
            msg.hash_join_node.setPartition_exprs(ExecExprSerializer.serializeList(partitionExprs));
        }
        msg.setFilter_null_value_columns(filter_null_value_columns);

        if (outputSlots != null) {
            msg.hash_join_node.setOutput_columns(outputSlots);
        }

        if (getCanLocalShuffle()) {
            msg.hash_join_node.setInterpolate_passthrough(sv.isHashJoinInterpolatePassthrough());
        }
        if (isSkewJoin) {
            msg.hash_join_node.setIs_skew_join(isSkewJoin);
        }
        if (commonSlotMap != null) {
            commonSlotMap.forEach((key, value) ->
                    msg.hash_join_node.putToCommon_slot_map(key.asInt(), ExecExprSerializer.serialize(value)));
        }
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalHashJoinNode hashJoinNode = new TNormalHashJoinNode();
        hashJoinNode.setJoin_op(ThriftEnumConverter.joinOperatorToThrift(getJoinOp()));
        hashJoinNode.setDistribution_mode(getDistrMode().toThrift());
        hashJoinNode.setEq_join_conjuncts(normalizer.normalizeExecExprs(eqJoinConjuncts));
        hashJoinNode.setOther_join_conjuncts(normalizer.normalizeExecExprs(otherJoinConjuncts));
        hashJoinNode.setPartition_exprs(normalizer.normalizeOrderedExecExprs(partitionExprs));
        hashJoinNode.setOutput_columns(normalizer.remapIntegerSlotIds(outputSlots));
        hashJoinNode.setLate_materialization(enableLateMaterialization);
        planNode.setHash_join_node(hashJoinNode);
        planNode.setNode_type(TPlanNodeType.HASH_JOIN_NODE);
        normalizeExecConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public void collectEquivRelation(FragmentNormalizer normalizer) {
        if (!joinOp.isSemiJoin() && !joinOp.isInnerJoin()) {
            return;
        }
        for (ExecBinaryPredicate eq : eqJoinConjuncts) {
            if (!eq.getOp().equals(BinaryType.EQ)) {
                continue;
            }
            SlotId lhsSlotId = ((ExecSlotRef) eq.getChild(0)).getSlotId();
            SlotId rhsSlotId = ((ExecSlotRef) eq.getChild(1)).getSlotId();
            normalizer.getEquivRelation().union(lhsSlotId, rhsSlotId);
        }
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        if (!joinOp.isInnerJoin() && joinOp.isSemiJoin()) {
            return false;
        }
        return super.extractConjunctsToNormalize(normalizer);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        if (joinOp.isRightJoin() || joinOp.isFullOuterJoin()) {
            return false;
        }

        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }
}

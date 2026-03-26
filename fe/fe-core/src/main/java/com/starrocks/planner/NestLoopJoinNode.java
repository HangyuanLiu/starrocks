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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.common.IdGenerator;
import com.starrocks.planner.expression.ExecBinaryPredicate;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprExplain;
import com.starrocks.planner.expression.ExecExprSerializer;
import com.starrocks.planner.expression.ExecExprUtils;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.planner.expression.ThriftEnumConverter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.thrift.TNestLoopJoinNode;
import com.starrocks.thrift.TNormalNestLoopJoinNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * NESTLOOP JOIN
 * Support all kinds of join type and join conjuncts
 */
public class NestLoopJoinNode extends JoinNode implements RuntimeFilterBuildNode {

    private static final Logger LOG = LogManager.getLogger(NestLoopJoinNode.class);

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner,
                            JoinOperator joinOp, List<ExecExpr> eqJoinConjuncts, List<ExecExpr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, eqJoinConjuncts, joinConjuncts);
    }

    /**
     * Build the filter if inner table contains only one row, which is a common case for scalar subquery
     */
    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator, DescriptorTable descTbl,
                                    ExecGroupSets execGroupSets) {
        if (!joinOp.isInnerJoin() && !joinOp.isLeftSemiJoin() && !joinOp.isRightJoin() && !joinOp.isCrossJoin()) {
            return;
        }
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        PlanNode buildStageNode = this.getChild(1);
        List<ExecExpr> allConjuncts = new ArrayList<>(otherJoinConjuncts);
        allConjuncts.addAll(getConjuncts());
        for (int i = 0; i < allConjuncts.size(); i++) {
            ExecExpr expr = allConjuncts.get(i);
            if (canBuildFilter(expr)) {
                ExecExpr left = expr.getChild(0);
                ExecExpr right = expr.getChild(1);

                RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
                rf.setFilterId(generator.getNextId().asInt());
                rf.setBuildPlanNodeId(getId().asInt());
                rf.setExprOrder(i);
                rf.setJoinMode(DistributionMode.BROADCAST);
                rf.setBuildCardinality(buildStageNode.getCardinality());
                rf.setOnlyLocal(true);
                rf.setBuildExpr(right);

                RuntimeFilterPushDownContext rfPushDownCtx =
                        new RuntimeFilterPushDownContext(rf, descTbl, execGroupSets);

                if (getChild(0).pushDownRuntimeFilters(rfPushDownCtx, left, probePartitionByExprs)) {
                    this.getBuildRuntimeFilters().add(rf);
                }
            }
        }
    }

    // Only binary op could build a filter
    // And some special cases are not suitable for build a filter, such as NOT_EQ
    private boolean canBuildFilter(ExecExpr joinExpr) {
        if (joinExpr.getNumChildren() != 2) {
            return false;
        }
        ExecExpr leftExpr = joinExpr.getChild(0);
        ExecExpr rightExpr = joinExpr.getChild(1);
        PlanNode leftChild = getChild(0);
        PlanNode rightChild = getChild(1);

        if (!(leftExpr instanceof ExecSlotRef)) {
            return false;
        }
        if (joinExpr instanceof ExecBinaryPredicate && ((ExecBinaryPredicate) joinExpr).getOp().isUnequivalence()) {
            return false;
        }
        if (!ExecExprUtils.isBoundByTupleIds(leftExpr, leftChild.getTupleIds())) {
            return false;
        }
        return ExecExprUtils.isBoundByTupleIds(rightExpr, rightChild.getTupleIds());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        Preconditions.checkState(CollectionUtils.isEmpty(eqJoinConjuncts));
        Preconditions.checkState(!joinOp.isRightSemiAntiJoin());
        msg.node_type = TPlanNodeType.NESTLOOP_JOIN_NODE;
        msg.nestloop_join_node = new TNestLoopJoinNode();
        msg.nestloop_join_node.join_op = ThriftEnumConverter.joinOperatorToThrift(joinOp);

        if (CollectionUtils.isNotEmpty(otherJoinConjuncts)) {
            for (ExecExpr e : otherJoinConjuncts) {
                msg.nestloop_join_node.addToJoin_conjuncts(ExecExprSerializer.serialize(e));
            }
            String sqlJoinPredicate = otherJoinConjuncts.stream().map(ExecExprExplain::explain)
                    .collect(Collectors.joining(","));
            msg.nestloop_join_node.setSql_join_conjuncts(sqlJoinPredicate);
        }
        SessionVariable sv = ConnectContext.get().getSessionVariable();
        if (getCanLocalShuffle()) {
            msg.nestloop_join_node.setInterpolate_passthrough(sv.isHashJoinInterpolatePassthrough());
        }


        if (!buildRuntimeFilters.isEmpty()) {
            msg.nestloop_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
        if (commonSlotMap != null) {
            commonSlotMap.forEach((key, value) ->
                    msg.nestloop_join_node.putToCommon_slot_map(key.asInt(), ExecExprSerializer.serialize(value)));
        }
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalNestLoopJoinNode nlJoinNode = new TNormalNestLoopJoinNode();
        nlJoinNode.setJoin_op(ThriftEnumConverter.joinOperatorToThrift(getJoinOp()));
        nlJoinNode.setJoin_conjuncts(normalizer.normalizeExecExprs(otherJoinConjuncts));
        planNode.setNestloop_join_node(nlJoinNode);
        planNode.setNode_type(TPlanNodeType.NESTLOOP_JOIN_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }
}

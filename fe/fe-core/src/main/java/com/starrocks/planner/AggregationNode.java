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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AggregationNode.java

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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.Pair;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprExplain;
import com.starrocks.planner.expression.ExecExprSerializer;
import com.starrocks.planner.expression.ExecExprUtils;
import com.starrocks.planner.expression.ExecFunctionCall;
import com.starrocks.planner.expression.ExecLiteral;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.thrift.TAggregationNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TNormalAggregationNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TRuntimeFilterDescription;
import com.starrocks.thrift.TStreamingPreaggregationMode;
import com.starrocks.type.Type;
import com.starrocks.type.UnknownType;
import org.apache.commons.collections.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.qe.SessionVariableConstants.AUTO;
import static com.starrocks.qe.SessionVariableConstants.FORCE_PREAGGREGATION;
import static com.starrocks.qe.SessionVariableConstants.FORCE_STREAMING;
import static com.starrocks.qe.SessionVariableConstants.LIMITED;

public class AggregationNode extends PlanNode implements RuntimeFilterBuildNode {
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private String streamingPreaggregationMode = "auto";

    private boolean useSortAgg = false;
    private boolean usePerBucketOptimize = false;

    private boolean withLocalShuffle = false;

    // direct set limit will introduce a limit on ExchangeNode
    private long localLimit = -1;

    // identicallyDistributed meanings the PlanNode above OlapScanNode are cases as follows:
    // 1. bucket shuffle join,
    // 2. colocate join,
    // 3. one-phase agg,
    // 4. 1st phaes of three-phase-agg(2nd phase of four-phase agg eliminated).
    // OlapScanNode and these PlanNodes have the same data partition policy.
    private boolean identicallyDistributed = false;

    private final List<RuntimeFilterDescription> buildRuntimeFilters = Lists.newArrayList();
    private boolean withRuntimeFilters = false;

    private List<Pair<ConstantOperator, ConstantOperator>> groupByMinMaxStats = Lists.newArrayList();

    // used for Top-N optimization for aggregation
    private SortInfo topNSortInfo;
    private long topNLimit = -1;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }

    // Unsets this node as requiring finalize. Only valid to call this if it is
    // currently marked as needing finalize.
    public void unsetNeedsFinalize() {
        Preconditions.checkState(needsFinalize);
        needsFinalize = false;
        updateplanNodeName();
    }

    /**
     * Sets this node as a preaggregation. Only valid to call this if it is not marked
     * as a preaggregation
     */
    public void setIsPreagg(boolean useStreamingPreAgg) {
        useStreamingPreagg = useStreamingPreAgg;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
    }

    public boolean isNeedsFinalize() {
        return needsFinalize;
    }

    /**
     * Have this node materialize the aggregation's intermediate tuple instead of
     * the output tuple.
     */
    public void setIntermediateTuple() {
        Preconditions.checkState(!tupleIds.isEmpty());
        Preconditions.checkState(tupleIds.get(0).equals(aggInfo.getOutputTupleId()));
        tupleIds.clear();
        tupleIds.add(aggInfo.getIntermediateTupleId());
    }

    public void setWithLocalShuffle(boolean withLocalShuffle) {
        this.withLocalShuffle = withLocalShuffle;
    }

    public void setStreamingPreaggregationMode(String mode) {
        this.streamingPreaggregationMode = mode;
    }

    public void setUseSortAgg(boolean useSortAgg) {
        this.useSortAgg = useSortAgg;
    }

    public void setUsePerBucketOptimize(boolean usePerBucketOptimize) {
        this.usePerBucketOptimize = usePerBucketOptimize;
    }

    public void setLocalLimit(long localLimit) {
        this.localLimit = localLimit;
    }

    public void setGroupByMinMaxStats(List<Pair<ConstantOperator, ConstantOperator>> groupByMinMaxStats) {
        this.groupByMinMaxStats = groupByMinMaxStats;
    }

    @Override
    public void disablePhysicalPropertyOptimize() {
        setUseSortAgg(false);
        setUsePerBucketOptimize(false);
    }

    @Override
    public void computeStats() {
    }

    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("AGGREGATE");
        sb.append(" (");
        if (aggInfo.isMerge()) {
            sb.append("merge");
        } else {
            sb.append("update");
        }
        if (needsFinalize) {
            sb.append(" finalize");
        } else {
            sb.append(" serialize");
        }
        sb.append(")");
        setPlanNodeName(sb.toString());
    }

    public void setIdenticallyDistributed(boolean identicallyDistributed) {
        this.identicallyDistributed = identicallyDistributed;
    }

    public boolean isIdenticallyDistributed() {
        return identicallyDistributed;
    }

    public void setTopNSortInfo(SortInfo topNSortInfo) {
        this.topNSortInfo = topNSortInfo;
    }

    public SortInfo getTopNSortInfo() {
        return topNSortInfo;
    }

    public void setTopNLimit(long topNLimit) {
        this.topNLimit = topNLimit;
    }

    public long getTopNLimit() {
        return topNLimit;
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("aggInfo", aggInfo.debugString()).addValue(
                super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;

        List<TExpr> aggregateFunctions = Lists.newArrayList();
        StringBuilder sqlAggFuncBuilder = new StringBuilder();
        // only serialize agg exprs that are being materialized
        for (ExecFunctionCall e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(ExecExprSerializer.serialize(e));
            if (sqlAggFuncBuilder.length() > 0) {
                sqlAggFuncBuilder.append(", ");
            }
            sqlAggFuncBuilder.append(ExecExprExplain.explain(e));
        }

        msg.agg_node =
                new TAggregationNode(
                        aggregateFunctions,
                        aggInfo.getIntermediateTupleId().asInt(),
                        aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setUse_streaming_preaggregation(useStreamingPreagg);
        if (sqlAggFuncBuilder.length() > 0) {
            msg.agg_node.setSql_aggregate_functions(sqlAggFuncBuilder.toString());
        }
        msg.agg_node.setUse_sort_agg(useSortAgg);
        msg.agg_node.setUse_per_bucket_optimize(usePerBucketOptimize);
        if (localLimit > 0) {
            Preconditions.checkState(!hasLimit());
            msg.limit = localLimit;
        }

        List<ExecExpr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            msg.agg_node.setGrouping_exprs(ExecExprSerializer.serializeList(groupingExprs));
            StringBuilder sqlGroupingKeysBuilder = new StringBuilder();
            for (ExecExpr e : groupingExprs) {
                if (sqlGroupingKeysBuilder.length() > 0) {
                    sqlGroupingKeysBuilder.append(", ");
                }
                sqlGroupingKeysBuilder.append(ExecExprExplain.explain(e));
            }
            if (sqlGroupingKeysBuilder.length() > 0) {
                msg.agg_node.setSql_grouping_keys(sqlGroupingKeysBuilder.toString());
            }

            List<ExecLiteral> minMaxStats = Lists.newArrayList();
            if (groupByMinMaxStats.size() == groupingExprs.size()) {
                for (int i = 0; i < groupingExprs.size(); i++) {
                    final ExecExpr expr = groupingExprs.get(i);

                    String min = groupByMinMaxStats.get(i).first.getVarchar();
                    String max = groupByMinMaxStats.get(i).second.getVarchar();

                    Type type = expr.getType();
                    // Use castToStrictly for decimals to preserve precision, castTo for others
                    Optional<ConstantOperator> minOp = type.isDecimalOfAnyVersion()
                            ? ConstantOperator.createVarchar(min).castToStrictly(type)
                            : ConstantOperator.createVarchar(min).castTo(type);
                    Optional<ConstantOperator> maxOp = type.isDecimalOfAnyVersion()
                            ? ConstantOperator.createVarchar(max).castToStrictly(type)
                            : ConstantOperator.createVarchar(max).castTo(type);
                    if (minOp.isEmpty() || maxOp.isEmpty()) {
                        break;
                    }
                    minMaxStats.add(new ExecLiteral(minOp.get(), type));
                    minMaxStats.add(new ExecLiteral(maxOp.get(), type));
                }
            }

            if (minMaxStats.size() == 2 * groupingExprs.size()) {
                msg.agg_node.setGroup_by_min_max(ExecExprSerializer.serializeList(minMaxStats));
            }
        }

        List<ExecExpr> intermediateAggrExprs = aggInfo.getIntermediateAggrExprs();
        if (intermediateAggrExprs != null && !intermediateAggrExprs.isEmpty()) {
            msg.agg_node.setIntermediate_aggr_exprs(ExecExprSerializer.serializeList(intermediateAggrExprs));
        }

        if (!buildRuntimeFilters.isEmpty()) {
            List<TRuntimeFilterDescription> tRuntimeFilterDescriptions =
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters);
            msg.agg_node.setBuild_runtime_filters(tRuntimeFilterDescriptions);
        }

        msg.agg_node.setHas_outer_join_child(hasNullableGenerateChild);
        if (streamingPreaggregationMode.equalsIgnoreCase(FORCE_STREAMING)) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_STREAMING);
        } else if (streamingPreaggregationMode.equalsIgnoreCase(FORCE_PREAGGREGATION)) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_PREAGGREGATION);
        } else if (streamingPreaggregationMode.equalsIgnoreCase(LIMITED)) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.LIMITED_MEM);
        } else {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.AUTO);
        }

        msg.agg_node.setAgg_func_set_version(FeConstants.AGG_FUNC_VERSION);
        msg.agg_node.setInterpolate_passthrough(
                useStreamingPreagg && ConnectContext.get().getSessionVariable().isInterpolatePassthrough());
        msg.agg_node.setEnable_pipeline_share_limit(
                ConnectContext.get().getSessionVariable().getEnableAggregationPipelineShareLimit());
    }

    protected String getDisplayLabelDetail() {
        if (useStreamingPreagg) {
            return "STREAMING";
        }
        return null;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        boolean verbose = TExplainLevel.VERBOSE.equals(detailLevel) || TExplainLevel.COSTS.equals(detailLevel);
        String nameDetail = getDisplayLabelDetail();
        if (nameDetail != null) {
            output.append(detailPrefix).append(nameDetail).append("\n");
        }
        if (aggInfo.getAggregateExprs() != null && !aggInfo.getMaterializedAggregateExprs().isEmpty()) {
            if (detailLevel == TExplainLevel.VERBOSE) {
                output.append(detailPrefix).append("aggregate: ");
            } else {
                output.append(detailPrefix).append("output: ");
            }
            output.append(verbose ? ExecExprExplain.verboseExplainList(aggInfo.getAggregateExprs())
                    : ExecExprExplain.explainList(aggInfo.getAggregateExprs())).append("\n");
        }
        // TODO: unify them
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(aggInfo.getGroupingExprs())) {
                output.append(detailPrefix).append("group by: ").append(
                        ExecExprExplain.verboseExplainList(aggInfo.getGroupingExprs())).append("\n");
            }
        } else {
            output.append(detailPrefix).append("group by: ").append(
                    ExecExprExplain.explainList(aggInfo.getGroupingExprs())).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(explainExpr(detailLevel, conjuncts))
                    .append("\n");
        }
        if (useSortAgg) {
            output.append(detailPrefix).append("sorted streaming: true\n");
        }
        if (detailLevel == TExplainLevel.VERBOSE && !AUTO.equalsIgnoreCase(streamingPreaggregationMode)) {
            output.append(detailPrefix).append("streaming preaggregation mode: ")
                    .append(streamingPreaggregationMode).append("\n");
        }

        if (withLocalShuffle) {
            output.append(detailPrefix).append("withLocalShuffle: true\n");
        }
        if (localLimit > 0) {
            output.append(detailPrefix).append("limit: ").append(localLimit).append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            if (!buildRuntimeFilters.isEmpty()) {
                output.append(detailPrefix).append("build runtime filters:\n");
                for (RuntimeFilterDescription rf : buildRuntimeFilters) {
                    output.append(detailPrefix).append("- ").append(rf.toExplainString(-1)).append("\n");
                }
            }
            if (!aggInfo.getGroupingExprs().isEmpty() &&
                    groupByMinMaxStats.size() == aggInfo.getGroupingExprs().size()) {
                output.append(detailPrefix).append("group by min-max stats:\n");
                for (Pair<ConstantOperator, ConstantOperator> stat : groupByMinMaxStats) {
                    output.append(detailPrefix).append("- ").append(stat.first).append(":").append(stat.second)
                            .append("\n");
                }
            }
        }

        return output.toString();
    }

    @Override
    public Optional<List<com.starrocks.planner.expression.ExecExpr>> candidatesOfSlotExpr(
            com.starrocks.planner.expression.ExecExpr expr,
            Function<com.starrocks.planner.expression.ExecExpr, Boolean> couldBound) {
        if (!couldBound.apply(expr)) {
            return Optional.empty();
        }
        if (!(expr instanceof com.starrocks.planner.expression.ExecSlotRef)) {
            return Optional.empty();
        }
        int probeSlotId = ((ExecSlotRef) expr).getSlotId().asInt();
        List<ExecExpr> newSlotExprs = Lists.newArrayList();
        for (ExecExpr gexpr : aggInfo.getGroupingExprs()) {
            if (!(gexpr instanceof ExecSlotRef)) {
                continue;
            }
            if (((ExecSlotRef) gexpr).getSlotId().asInt() == probeSlotId) {
                newSlotExprs.add(gexpr);
            }
        }
        return newSlotExprs.size() > 0 ? Optional.of(newSlotExprs) : Optional.empty();
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterPushDownContext context,
                                          com.starrocks.planner.expression.ExecExpr probeExpr,
                                          List<com.starrocks.planner.expression.ExecExpr> partitionByExprs) {
        RuntimeFilterDescription description = context.getDescription();
        DescriptorTable descTbl = context.getDescTbl();
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!couldBound(probeExpr, description, descTbl)) {
            return false;
        }

        Function<com.starrocks.planner.expression.ExecExpr, Boolean> couldBoundChecker =
                couldBound(description, descTbl);
        return pushdownRuntimeFilterForChildOrAccept(context, probeExpr,
                candidatesOfSlotExpr(probeExpr, couldBoundChecker),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs, couldBoundForPartitionExpr()), 0, true);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return !withRuntimeFilters && getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    private void disableCacheIfHighCardinalityGroupBy(FragmentNormalizer normalizer) {
        if (ConnectContext.get() == null || getCardinality() == -1) {
            return;
        }
        long cardinalityLimit = ConnectContext.get().getSessionVariable().getQueryCacheAggCardinalityLimit();
        long cardinality = getCardinality();
        if (cardinality < cardinalityLimit || aggInfo.getGroupingExprs().isEmpty()) {
            return;
        }
        List<ExecExpr> groupByExprs = aggInfo.getGroupingExprs();
        if (groupByExprs.size() > 3) {
            normalizer.setUncacheable(true);
        }
        List<ExecSlotRef> slotRefs = groupByExprs.stream()
                .filter(e -> e instanceof ExecSlotRef && e.getType().isStringType())
                .map(e -> (ExecSlotRef) e).collect(Collectors.toList());
        // we assume that if there exists a very high cardinality of string-typed group-by columns whose average length is
        // greater than 24 bytes(it is equivalent to three bigint-typed group-by columns), then cache populating penalty
        // is unacceptable.
        List<ColumnStatistic> stringColumnStatistics = slotRefs.stream()
                .map(slot -> columnStatistics.get(new ColumnRefOperator(slot.getSlotId().asInt(),
                        UnknownType.UNKNOWN_TYPE, "key", false)))
                .filter(stat -> stat != null && !stat.isUnknown() &&
                        stat.getAverageRowSize() * stat.getDistinctValuesCount() > 24 * cardinalityLimit)
                .collect(Collectors.toList());
        if (!stringColumnStatistics.isEmpty()) {
            normalizer.setUncacheable(true);
        }
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        List<com.starrocks.planner.expression.ExecExpr> conjuncts = normalizer.getConjunctsByPlanNodeId(this);
        normalizer.filterOutPartColRangePredicates(getId(), conjuncts,
                FragmentNormalizer.getExecExprSlotIdSet(aggInfo.getGroupingExprs()));
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        disableCacheIfHighCardinalityGroupBy(normalizer);
        TNormalAggregationNode aggrNode = new TNormalAggregationNode();
        TupleId tupleId = needsFinalize ? aggInfo.getOutputTupleId() : aggInfo.getIntermediateTupleId();
        List<SlotId> slotIds = normalizer.getExecPlan().getDescTbl().getTupleDesc(tupleId).getSlots()
                .stream().map(SlotDescriptor::getId).collect(Collectors.toList());

        List<ExecExpr> groupingExprs = aggInfo.getGroupingExprs();
        Map<SlotId, ExecExpr> slotIdsAndGroupingExprs = Maps.newHashMap();
        int numGroupingExprs = (groupingExprs == null || groupingExprs.isEmpty()) ? 0 : groupingExprs.size();

        IntStream.range(0, numGroupingExprs).forEach(i ->
                slotIdsAndGroupingExprs.put(slotIds.get(i), groupingExprs.get(i)));
        Pair<List<Integer>, List<ByteBuffer>> remappedGroupExprs =
                normalizer.normalizeSlotIdsAndExprs(slotIdsAndGroupingExprs, true);
        aggrNode.setGrouping_exprs(remappedGroupExprs.second);

        Map<SlotId, ExecExpr> slotIdsAndAggExprs = Maps.newHashMap();
        List<ExecFunctionCall> aggExprs = aggInfo.getMaterializedAggregateExprs();
        int numAggExprs = (aggExprs == null || aggExprs.isEmpty()) ? 0 : aggExprs.size();
        IntStream.range(0, numAggExprs).forEach(i ->
                slotIdsAndAggExprs.put(slotIds.get(i + numGroupingExprs), aggExprs.get(i)));

        normalizer.addSlotsUseAggColumnsExec(slotIdsAndAggExprs);
        normalizer.disableMultiversionIfExecExprsUseAggColumns(groupingExprs);

        Pair<List<Integer>, List<ByteBuffer>> remappedAggExprs =
                normalizer.normalizeSlotIdsAndExprs(slotIdsAndAggExprs, true);
        aggrNode.setAggregate_functions(remappedAggExprs.second);

        aggrNode.setIntermediate_tuple_id(normalizer.remapTupleId(aggInfo.getIntermediateTupleId()).asInt());
        aggrNode.setOutput_tuple_id(normalizer.remapTupleId(aggInfo.getOutputTupleId()).asInt());
        aggrNode.setNeed_finalize(needsFinalize);
        aggrNode.setUse_streaming_preaggregation(useStreamingPreagg);
        aggrNode.setHas_outer_join_child(hasNullableGenerateChild);
        if (streamingPreaggregationMode.equalsIgnoreCase("force_streaming")) {
            aggrNode.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_STREAMING);
        } else if (streamingPreaggregationMode.equalsIgnoreCase("force_preaggregation")) {
            aggrNode.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_PREAGGREGATION);
        } else {
            aggrNode.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.AUTO);
        }
        aggrNode.setAgg_func_set_version(FeConstants.AGG_FUNC_VERSION);
        planNode.setNode_type(TPlanNodeType.AGGREGATION_NODE);
        planNode.setAgg_node(aggrNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        final List<ExecExpr> groupingExprs = aggInfo.getGroupingExprs();
        final List<ExecFunctionCall> aggExprs = aggInfo.getMaterializedAggregateExprs();
        int numGroupingExprs = groupingExprs != null ? groupingExprs.size() : 0;
        int numAggExprs = aggExprs != null ? aggExprs.size() : 0;
        TupleId tupleId = needsFinalize ? aggInfo.getOutputTupleId() : aggInfo.getIntermediateTupleId();
        return descriptorTable.getTupleDesc(tupleId).getSlots().subList(0, numGroupingExprs + numAggExprs)
                .stream().map(SlotDescriptor::getId).collect(Collectors.toList());
    }

    @Override
    public boolean needCollectExecStats() {
        return true;
    }

    @Override
    public List<RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator, DescriptorTable descTbl,
                                    ExecGroupSets execGroupSets) {
        SessionVariable sv = ConnectContext.get().getSessionVariable();
        // RF push down group by one column
        if (limit > 0 && limit < sv.getAggInFilterLimit() && !aggInfo.getAggregateExprs().isEmpty() &&
                !aggInfo.getGroupingExprs().isEmpty()) {
            ExecExpr groupingExpr = aggInfo.getGroupingExprs().get(0);
            pushDownUnaryInRuntimeFilter(generator, groupingExpr, descTbl, execGroupSets, 0);
        }
        // generate topn runtime filter
        if (sv.getEnableTopNRuntimeFilter() && topNSortInfo != null
                && !topNSortInfo.getOrderingExprs().isEmpty()) {
            com.starrocks.planner.expression.ExecExpr topnExpr = topNSortInfo.getOrderingExprs().get(0);
            pushDownUnaryTopNRuntimeFilter(generator, topnExpr, descTbl, execGroupSets, 0);
        }
        withRuntimeFilters = !buildRuntimeFilters.isEmpty();
    }

    private void pushDownUnaryAggInRuntimeFilter(IdGenerator<RuntimeFilterId> generator,
                                                 com.starrocks.planner.expression.ExecExpr expr,
                                                 DescriptorTable descTbl,
                                                 ExecGroupSets execGroupSets,
                                                 RuntimeFilterDescription.RuntimeFilterType type,
                                                 int exprOrder,
                                                 JoinNode.DistributionMode mode) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
        rf.setFilterId(generator.getNextId().asInt());
        rf.setBuildPlanNodeId(getId().asInt());
        rf.setExprOrder(exprOrder);
        rf.setJoinMode(mode);
        rf.setBuildExpr(expr);
        rf.setRuntimeFilterType(type);
        rf.setEqualCount(1);
        RuntimeFilterPushDownContext rfPushDownCtx = new RuntimeFilterPushDownContext(rf, descTbl, execGroupSets);
        for (PlanNode child : children) {
            if (child.pushDownRuntimeFilters(rfPushDownCtx, expr, Lists.newArrayList())) {
                this.buildRuntimeFilters.add(rf);
            }
        }
    }

    private void pushDownUnaryInRuntimeFilter(IdGenerator<RuntimeFilterId> generator,
                                              com.starrocks.planner.expression.ExecExpr expr,
                                              DescriptorTable descTbl,
                                              ExecGroupSets execGroupSets, int exprOrder) {
        pushDownUnaryAggInRuntimeFilter(generator, expr, descTbl, execGroupSets,
                RuntimeFilterDescription.RuntimeFilterType.AGG_IN_FILTER, exprOrder,
                JoinNode.DistributionMode.PARTITIONED);
    }

    private void pushDownUnaryAggTopNRuntimeFilter(IdGenerator<RuntimeFilterId> generator,
                                                 com.starrocks.planner.expression.ExecExpr expr,
                                                 DescriptorTable descTbl,
                                                 ExecGroupSets execGroupSets,
                                                 RuntimeFilterDescription.RuntimeFilterType type,
                                                 int exprOrder,
                                                 JoinNode.DistributionMode mode) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
        rf.setFilterId(generator.getNextId().asInt());
        rf.setBuildPlanNodeId(getId().asInt());
        rf.setExprOrder(exprOrder);
        rf.setJoinMode(mode);
        rf.setBuildExpr(expr);
        rf.setRuntimeFilterType(type);
        rf.setOnlyLocal(true);
        rf.setSortInfo(topNSortInfo);
        rf.setTopN(topNLimit);
        rf.setEqualCount(1);
        RuntimeFilterPushDownContext rfPushDownCtx = new RuntimeFilterPushDownContext(rf, descTbl, execGroupSets);
        for (PlanNode child : children) {
            if (child.pushDownRuntimeFilters(rfPushDownCtx, expr, Lists.newArrayList())) {
                this.buildRuntimeFilters.add(rf);
            }
        }
    }

    private void pushDownUnaryTopNRuntimeFilter(IdGenerator<RuntimeFilterId> generator,
                                                com.starrocks.planner.expression.ExecExpr expr,
                                                DescriptorTable descTbl, ExecGroupSets execGroupSets,
                                                int exprOrder) {
        pushDownUnaryAggTopNRuntimeFilter(generator, expr, descTbl, execGroupSets,
                RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER, exprOrder,
                JoinNode.DistributionMode.BROADCAST);
    }

    @Override
    public void clearBuildRuntimeFilters() {
        buildRuntimeFilters.clear();
    }
}

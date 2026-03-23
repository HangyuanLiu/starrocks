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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AggregateInfoBase.java

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
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprExplain;
import com.starrocks.planner.expression.ExecFunctionCall;

import java.util.ArrayList;

/**
 * Base class for AggregateInfo and AnalyticInfo containing the intermediate and output
 * tuple descriptors as well as their smaps for evaluating aggregate functions.
 */
public abstract class AggregateInfoBase {

    // For aggregations: All unique grouping expressions from a select block.
    // For analytics: Empty.
    protected ArrayList<ExecExpr> groupingExprs_;

    // For aggregations: All unique aggregate expressions from a select block.
    // For analytics: The results of AnalyticExpr.getFnCall() for the unique
    // AnalyticExprs of a select block.
    protected ArrayList<ExecFunctionCall> aggregateExprs_;

    // The tuple into which the intermediate output of an aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' update()/merge() symbols materialize their output, i.e., slots
    // of the aggregate functions' intermediate types.
    // Identical to outputTupleDesc_ if no aggregateExpr has an output type that is
    // different from its intermediate type.
    protected TupleDescriptor intermediateTupleDesc_;

    // The tuple into which the final output of the aggregation is materialized.
    // Contains groupingExprs.size() + aggregateExprs.size() slots, the first of which
    // contain the values of the grouping exprs, followed by slots into which the
    // aggregateExprs' finalize() symbol write its result, i.e., slots of the aggregate
    // functions' output types.
    protected TupleDescriptor outputTupleDesc_;

    // For aggregation: indices into aggregate exprs for that need to be materialized
    // For analytics: indices into the analytic exprs and their corresponding aggregate
    // exprs that need to be materialized.
    // Populated in materializeRequiredSlots() which must be implemented by subclasses.
    protected ArrayList<Integer> materializedAggSlots = Lists.newArrayList();

    protected AggregateInfoBase(ArrayList<ExecExpr> groupingExprs,
                                ArrayList<ExecFunctionCall> aggExprs) {
        Preconditions.checkState(groupingExprs != null || aggExprs != null);
        groupingExprs_ =
                groupingExprs != null ? cloneExecExprList(groupingExprs) : new ArrayList<>();
        aggregateExprs_ =
                aggExprs != null ? cloneExecFunctionCallList(aggExprs) : new ArrayList<>();
    }

    /**
     * C'tor for cloning.
     */
    protected AggregateInfoBase(AggregateInfoBase other) {
        groupingExprs_ =
                (other.groupingExprs_ != null) ? cloneExecExprList(other.groupingExprs_) : null;
        aggregateExprs_ =
                (other.aggregateExprs_ != null) ? cloneExecFunctionCallList(other.aggregateExprs_) : null;
        intermediateTupleDesc_ = other.intermediateTupleDesc_;
        outputTupleDesc_ = other.outputTupleDesc_;
        materializedAggSlots = Lists.newArrayList(other.materializedAggSlots);
    }

    private static ArrayList<ExecExpr> cloneExecExprList(ArrayList<ExecExpr> exprs) {
        ArrayList<ExecExpr> result = new ArrayList<>(exprs.size());
        for (ExecExpr expr : exprs) {
            result.add(expr.clone());
        }
        return result;
    }

    private static ArrayList<ExecFunctionCall> cloneExecFunctionCallList(ArrayList<ExecFunctionCall> exprs) {
        ArrayList<ExecFunctionCall> result = new ArrayList<>(exprs.size());
        for (ExecFunctionCall expr : exprs) {
            result.add((ExecFunctionCall) expr.clone());
        }
        return result;
    }

    public ArrayList<ExecExpr> getGroupingExprs() {
        return groupingExprs_;
    }

    public ArrayList<ExecFunctionCall> getAggregateExprs() {
        return aggregateExprs_;
    }

    public TupleId getIntermediateTupleId() {
        return intermediateTupleDesc_.getId();
    }

    public TupleId getOutputTupleId() {
        return outputTupleDesc_.getId();
    }

    public void setOutputTupleDesc(TupleDescriptor tupleDesc) {
        this.outputTupleDesc_ = tupleDesc;
    }

    public boolean requiresIntermediateTuple() {
        Preconditions.checkNotNull(intermediateTupleDesc_);
        Preconditions.checkNotNull(outputTupleDesc_);
        return intermediateTupleDesc_ != outputTupleDesc_;
    }

    public String debugString() {
        StringBuilder out = new StringBuilder();
        out.append(MoreObjects.toStringHelper(this)
                .add("grouping_exprs", ExecExprExplain.explainList(groupingExprs_))
                .add("aggregate_exprs", ExecExprExplain.explainList(aggregateExprs_))
                .add("intermediate_tuple", (intermediateTupleDesc_ == null)
                        ? "null" : intermediateTupleDesc_.debugString())
                .add("output_tuple", (outputTupleDesc_ == null)
                        ? "null" : outputTupleDesc_.debugString())
                .toString());
        return out.toString();
    }

    protected abstract String tupleDebugName();
}

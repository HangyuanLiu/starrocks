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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AggregateInfo.java

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecFunctionCall;

import java.util.ArrayList;
import java.util.List;

public final class AggregateInfo extends AggregateInfoBase {

    public enum AggPhase {
        FIRST,
        FIRST_MERGE,
        SECOND,
        SECOND_MERGE;

        public boolean isMerge() {
            return this == FIRST_MERGE || this == SECOND_MERGE;
        }
    }

    // created by createMergeAggInfo()
    private AggregateInfo mergeAggInfo_;

    // created by createDistinctAggInfo()
    private AggregateInfo secondPhaseDistinctAggInfo_;

    private final AggPhase aggPhase_;

    // if set, a subset of groupingExprs_; set and used during planning
    private List<ExecExpr> partitionExprs_;

    private List<ExecExpr> intermediateAggrExprs = Lists.newArrayList();

    // C'tor creates copies of groupingExprs and aggExprs.
    @VisibleForTesting
    public AggregateInfo(ArrayList<ExecExpr> groupingExprs,
                         ArrayList<ExecFunctionCall> aggExprs, AggPhase aggPhase) {
        this(groupingExprs, aggExprs, aggPhase, false);
    }

    private AggregateInfo(ArrayList<ExecExpr> groupingExprs,
                          ArrayList<ExecFunctionCall> aggExprs, AggPhase aggPhase, boolean isMultiDistinct) {
        super(groupingExprs, aggExprs);
        aggPhase_ = aggPhase;
    }

    /**
     * C'tor for cloning.
     */
    private AggregateInfo(AggregateInfo other) {
        super(other);
        if (other.mergeAggInfo_ != null) {
            mergeAggInfo_ = other.mergeAggInfo_.clone();
        }
        if (other.secondPhaseDistinctAggInfo_ != null) {
            secondPhaseDistinctAggInfo_ = other.secondPhaseDistinctAggInfo_.clone();
        }
        aggPhase_ = other.aggPhase_;
        if (other.partitionExprs_ != null) {
            partitionExprs_ = Lists.newArrayList();
            for (ExecExpr e : other.partitionExprs_) {
                partitionExprs_.add(e.clone());
            }
        }
    }

    public List<ExecExpr> getPartitionExprs() {
        return partitionExprs_;
    }

    public void setPartitionExprs(List<ExecExpr> exprs) {
        partitionExprs_ = exprs;
    }


    public ArrayList<ExecFunctionCall> getMaterializedAggregateExprs() {
        ArrayList<ExecFunctionCall> result = Lists.newArrayList();
        for (Integer i : materializedAggSlots) {
            result.add(aggregateExprs_.get(i));
        }
        return result;
    }

    public boolean isMerge() {
        return aggPhase_.isMerge();
    }

    public void setIntermediateAggrExprs(List<ExecExpr> intermediateAggrExprs) {
        this.intermediateAggrExprs = intermediateAggrExprs;
    }

    public List<ExecExpr> getIntermediateAggrExprs() { return intermediateAggrExprs; }

    public String debugString() {
        StringBuilder out = new StringBuilder(super.debugString());
        out.append(MoreObjects.toStringHelper(this)
                .add("phase", aggPhase_)
                .toString());
        if (mergeAggInfo_ != this && mergeAggInfo_ != null) {
            out.append("\nmergeAggInfo:\n" + mergeAggInfo_.debugString());
        }
        if (secondPhaseDistinctAggInfo_ != null) {
            out.append("\nsecondPhaseDistinctAggInfo:\n"
                    + secondPhaseDistinctAggInfo_.debugString());
        }
        return out.toString();
    }

    @Override
    protected String tupleDebugName() {
        return "agg-tuple";
    }

    @Override
    public AggregateInfo clone() {
        return new AggregateInfo(this);
    }

    /**
     * Below function is added by new analyzer
     */
    public static AggregateInfo create(
            ArrayList<ExecExpr> groupingExprs, ArrayList<ExecFunctionCall> aggExprs,
            TupleDescriptor tupleDesc, TupleDescriptor intermediateTupleDesc, AggPhase phase) {
        AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, phase);
        result.outputTupleDesc_ = tupleDesc;
        result.intermediateTupleDesc_ = intermediateTupleDesc;

        for (int i = 0; i < result.getAggregateExprs().size(); ++i) {
            result.materializedAggSlots.add(i);
        }

        return result;
    }
}

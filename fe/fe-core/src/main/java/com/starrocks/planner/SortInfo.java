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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SortInfo.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecExprSerializer;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.thrift.TSortInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 */
public class SortInfo {

    // Only used in local partition topn
    private List<ExecExpr> partitionExprs_;
    private long partitionLimit_;
    private List<ExecExpr> orderingExprs_;
    private final List<Boolean> isAscOrder_;
    // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
    private final List<Boolean> nullsFirstParams_;
    // The single tuple that is materialized, sorted, and output by a sort operator
    // (i.e. SortNode or TopNNode)
    private TupleDescriptor sortTupleDesc_;
    // Input expressions materialized into sortTupleDesc_. One expr per slot in
    // sortTupleDesc_.
    private List<ExecExpr> sortTupleSlotExprs_;

    private TupleDescriptor preAggTupleDesc_;

    public SortInfo(List<ExecExpr> partitionExprs, long partitionLimit, List<ExecExpr> orderingExprs,
                    List<Boolean> isAscOrder, List<Boolean> nullsFirstParams) {
        Preconditions.checkArgument(orderingExprs.size() == isAscOrder.size());
        Preconditions.checkArgument(orderingExprs.size() == nullsFirstParams.size());
        partitionExprs_ = partitionExprs;
        partitionLimit_ = partitionLimit;
        orderingExprs_ = orderingExprs;
        isAscOrder_ = isAscOrder;
        nullsFirstParams_ = nullsFirstParams;
    }

    /**
     * C'tor for cloning.
     */
    private SortInfo(SortInfo other) {
        partitionExprs_ = cloneExecExprList(other.partitionExprs_);
        partitionLimit_ = other.partitionLimit_;
        orderingExprs_ = cloneExecExprList(other.orderingExprs_);
        isAscOrder_ = Lists.newArrayList(other.isAscOrder_);
        nullsFirstParams_ = Lists.newArrayList(other.nullsFirstParams_);
        sortTupleDesc_ = other.sortTupleDesc_;
        if (other.sortTupleSlotExprs_ != null) {
            sortTupleSlotExprs_ = cloneExecExprList(other.sortTupleSlotExprs_);
        }
    }

    private static List<ExecExpr> cloneExecExprList(List<ExecExpr> exprs) {
        if (exprs == null) {
            return null;
        }
        List<ExecExpr> result = new ArrayList<>(exprs.size());
        for (ExecExpr expr : exprs) {
            result.add(expr.clone());
        }
        return result;
    }

    /**
     * Sets sortTupleDesc_, which is the internal row representation to be materialized and
     * sorted. The source exprs of the slots in sortTupleDesc_ are changed to those in
     * tupleSlotExprs.
     */
    public void setMaterializedTupleInfo(
            TupleDescriptor tupleDesc, List<ExecExpr> tupleSlotExprs) {
        Preconditions.checkState(tupleDesc.getSlots().size() == tupleSlotExprs.size());
        sortTupleDesc_ = tupleDesc;
        sortTupleSlotExprs_ = tupleSlotExprs;
        for (int i = 0; i < sortTupleDesc_.getSlots().size(); ++i) {
            SlotDescriptor slotDesc = sortTupleDesc_.getSlots().get(i);
            slotDesc.setSourceExecExpr(sortTupleSlotExprs_.get(i));
        }
    }

    public List<ExecExpr> getPartitionExprs() {
        return partitionExprs_;
    }

    public long getPartitionLimit() {
        return partitionLimit_;
    }

    public List<ExecExpr> getOrderingExprs() {
        return orderingExprs_;
    }

    public List<Boolean> getIsAscOrder() {
        return isAscOrder_;
    }

    public List<ExecExpr> getSortTupleSlotExprs() {
        return sortTupleSlotExprs_;
    }

    public TupleDescriptor getSortTupleDescriptor() {
        return sortTupleDesc_;
    }

    /**
     * Gets the list of booleans indicating whether nulls come first or last, independent
     * of asc/desc.
     */
    public List<Boolean> getNullsFirst() {
        Preconditions.checkState(orderingExprs_.size() == nullsFirstParams_.size());
        List<Boolean> nullsFirst = Lists.newArrayList();
        for (int i = 0; i < orderingExprs_.size(); ++i) {
            nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams_.get(i)
            ));
        }
        return nullsFirst;
    }

    public void setPreAggTupleDesc_(TupleDescriptor preAggTupleDesc_) {
        this.preAggTupleDesc_ = preAggTupleDesc_;
    }

    public TupleDescriptor getPreAggTupleDesc_() {
        return preAggTupleDesc_;
    }

    @Override
    public SortInfo clone() {
        return new SortInfo(this);
    }

    public TSortInfo toTSortInfo() {
        return new TSortInfo(ExecExprSerializer.serializeList(getOrderingExprs()), getIsAscOrder(), getNullsFirst());
    }
}

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.ast.PolicyType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PolicyContext {
    @SerializedName(value = "MaskingPolicyContext")
    private final Map<String, ColumnMaskingPolicyContext> maskingPolicyApply;

    @SerializedName(value = "RowAccessPolicyContext")
    private final List<RowAccessPolicyContext> rowAccessPolicyApply;

    private final ReentrantReadWriteLock contextLock;

    public PolicyContext() {
        rowAccessPolicyApply = new ArrayList<>();
        maskingPolicyApply = new HashMap<>();
        contextLock = new ReentrantReadWriteLock();
    }

    public void applyMaskingPolicy(String maskingColumn, ColumnMaskingPolicyContext columnMaskingPolicyContext) {
        maskingPolicyApply.put(maskingColumn, columnMaskingPolicyContext);
    }

    public void revokeMaskingPolicy(String maskingColumn) {
        maskingPolicyApply.remove(maskingColumn);
    }

    public Map<String, ColumnMaskingPolicyContext> getMaskingPolicyApply() {
        return maskingPolicyApply;
    }

    public void addRowAccessPolicy(RowAccessPolicyContext withRowAccessPolicy) {
        rowAccessPolicyApply.add(withRowAccessPolicy);
    }

    public List<RowAccessPolicyContext> getRowAccessPolicyApply() {
        return rowAccessPolicyApply;
    }

    public void revokeMaskingPolicy(Long policyId) {
        contextLock.writeLock().lock();

        try {
            Iterator<Map.Entry<String, ColumnMaskingPolicyContext>> maskingPolicyContextIterator
                    = maskingPolicyApply.entrySet().iterator();
            while (maskingPolicyContextIterator.hasNext()) {
                Map.Entry<String, ColumnMaskingPolicyContext> entry = maskingPolicyContextIterator.next();
                ColumnMaskingPolicyContext withColumnMaskingPolicy = entry.getValue();
                if (withColumnMaskingPolicy.getPolicyId().equals(policyId)) {
                    maskingPolicyContextIterator.remove();
                }
            }
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public void revokeRowAccessPolicy(Long policyId) {
        contextLock.writeLock().lock();

        try {
            Iterator<RowAccessPolicyContext> rowAccessPolicyContextIterator = rowAccessPolicyApply.iterator();
            while (rowAccessPolicyContextIterator.hasNext()) {
                RowAccessPolicyContext withRowAccessPolicy = rowAccessPolicyContextIterator.next();
                if (withRowAccessPolicy.getPolicyId().equals(policyId)) {
                    rowAccessPolicyContextIterator.remove();
                }
            }
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public void clearRowAccessPolicy() {
        rowAccessPolicyApply.clear();
    }

    public boolean hasApplyPolicy(PolicyType policyType, Long policyId) {
        if (policyType.equals(PolicyType.COLUMN_MASKING)) {
            return maskingPolicyApply.values().stream().anyMatch(
                    columnMaskingPolicyContext -> columnMaskingPolicyContext.policyId.equals(policyId));
        } else {
            return rowAccessPolicyApply.stream().anyMatch(
                    rowAccessPolicyContext -> rowAccessPolicyContext.policyId.equals(policyId));
        }
    }
}
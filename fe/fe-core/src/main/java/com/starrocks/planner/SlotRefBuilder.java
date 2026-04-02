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

import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.expression.SlotRef;

/**
 * Utility for creating SlotRef from fe-core types (SlotDescriptor, Column).
 * Since SlotRef lives in fe-parser and cannot reference fe-core types directly,
 * this builder bridges the gap.
 */
public class SlotRefBuilder {

    public static SlotRef fromDescriptor(SlotDescriptor desc) {
        String colName = desc.getLabel();
        SlotRef ref = new SlotRef(null, colName != null ? colName : "");
        ref.setLabel(null); // Match old SlotRef(SlotDescriptor) behavior: label=null
        populateFromDescriptor(ref, desc);
        return ref;
    }

    public static SlotRef fromDescriptor(String label, SlotDescriptor desc) {
        String colName = desc.getLabel();
        SlotRef ref = new SlotRef(null, colName != null ? colName : "");
        ref.setLabel(label); // Override label
        populateFromDescriptor(ref, desc);
        return ref;
    }

    public static SlotRef fromSlotId(SlotId slotId) {
        SlotRef ref = new SlotRef(null, "");
        ref.setSlotId(slotId.asInt());
        ref.analysisDone();
        return ref;
    }

    private static void populateFromDescriptor(SlotRef ref, SlotDescriptor desc) {
        ref.setSlotId(desc.getId().asInt());
        if (desc.getParent() != null) {
            ref.setTupleId(desc.getParent().getId().asInt());
        }
        com.starrocks.type.Type type = desc.getType();
        if (type.isChar()) {
            type = com.starrocks.type.VarcharType.VARCHAR;
        }
        ref.setType(type);
        ref.setNullable(desc.getIsNullable());
        // Delegate isNullable() to the descriptor so later descriptor modifications are reflected
        ref.setNullableSupplier(desc::getIsNullable);
        // Delegate sourceExprs to the descriptor for explain formatting
        ref.setSourceExprsSupplier(desc::getSourceExprs);
        // Store descriptor reference for fe-core explain visitors that need original type/nullable
        ref.setDescriptorRef(desc);
        Column col = desc.getColumn();
        if (col != null) {
            ref.setColumnId(col.getColumnId().toString());
        }
        ref.analysisDone();
    }
}

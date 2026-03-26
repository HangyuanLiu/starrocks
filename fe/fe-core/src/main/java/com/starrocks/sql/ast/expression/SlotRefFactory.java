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

package com.starrocks.sql.ast.expression;

import com.starrocks.planner.SlotDescriptor;
import com.starrocks.type.VarcharType;

/**
 * Factory for creating SlotRef instances from planner-level descriptors.
 * Replaces the old AnalyzedSlotRef(SlotDescriptor) constructor pattern.
 */
public class SlotRefFactory {

    /**
     * Create a SlotRef from a SlotDescriptor, populating slotId, tupleId, type, and nullable.
     * This replaces {@code new AnalyzedSlotRef(SlotDescriptor)}.
     */
    public static SlotRef fromDescriptor(SlotDescriptor desc) {
        SlotRef ref = new SlotRef(null, desc.getLabel());
        ref.setSlotId(desc.getId().asInt());
        if (desc.getParent() != null) {
            ref.setTupleId(desc.getParent().getId().asInt());
        }
        ref.setType(desc.getType());
        ref.setOriginType(desc.getOriginType());
        ref.setLabel(null);
        if (ref.getType().isChar()) {
            ref.setType(VarcharType.VARCHAR);
        }
        ref.setNullable(desc.getIsNullable());
        ref.analysisDone();
        return ref;
    }

    /**
     * Create a SlotRef from a SlotDescriptor with a custom label.
     * This replaces {@code new AnalyzedSlotRef(String, SlotDescriptor)}.
     */
    public static SlotRef fromDescriptor(String label, SlotDescriptor desc) {
        SlotRef ref = fromDescriptor(desc);
        ref.setLabel(label);
        return ref;
    }

    /**
     * Create a SlotRef with only a slotId (minimal, for slot-id-only usages).
     * This replaces {@code new AnalyzedSlotRef(SlotId)}.
     */
    public static SlotRef fromSlotId(int slotId) {
        SlotRef ref = new SlotRef(null, "");
        ref.setSlotId(slotId);
        ref.analysisDone();
        return ref;
    }

    /**
     * Create a SlotRef from a slotId, name, type, and nullable flag.
     * This replaces the pattern {@code new AnalyzedSlotRef(new SlotDescriptor(new SlotId(...), name, type, nullable))}.
     */
    public static SlotRef create(int slotId, String name, com.starrocks.type.Type type, boolean nullable) {
        SlotRef ref = new SlotRef(null, name);
        ref.setSlotId(slotId);
        ref.setType(type);
        ref.setLabel(null);
        if (ref.getType().isChar()) {
            ref.setType(VarcharType.VARCHAR);
        }
        ref.setNullable(nullable);
        ref.analysisDone();
        return ref;
    }

    /**
     * Populate a plain SlotRef with information from a SlotDescriptor.
     * This replaces the old {@code AnalyzedSlotRef.setOrAttachDesc(slotRef, desc)} pattern.
     */
    public static void populateFromDescriptor(SlotRef slotRef, SlotDescriptor desc) {
        slotRef.setSlotId(desc.getId().asInt());
        if (desc.getParent() != null) {
            slotRef.setTupleId(desc.getParent().getId().asInt());
        }
        slotRef.setType(desc.getType());
        slotRef.setNullable(desc.getIsNullable());
        slotRef.analysisDone();
    }
}

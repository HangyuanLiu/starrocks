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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SlotRef.java

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

package com.starrocks.sql.ast.expression;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class SlotRef extends Expr {
    public static final String LAMBDA_FUNC_TABLE = "__LAMBDA_TABLE";

    private QualifiedName tblName;
    private String colName;
    private String columnId;
    //label/isBackQuoted used in toSql
    private String label;
    private boolean isBackQuoted = false;

    private QualifiedName qualifiedName;

    // Only Struct Type need this field
    // Record access struct subfield path position
    // Example: struct type: col: STRUCT<c1: INT, c2: STRUCT<c1: INT, c2: DOUBLE>>,
    // We execute sql: `SELECT col FROM table`, the usedStructField value is [].
    // We execute sql: `SELECT col.c2 FROM table`, the usedStructFieldPos value is [1].
    // We execute sql: `SELECT col.c2.c1 FROM table`, the usedStructFieldPos value is [1, 0].
    private ImmutableList<Integer> usedStructFieldPos;

    // now it is used in Analyzer phase of creating mv to decide the field nullable of mv
    // can not use desc because the slotId is unknown in Analyzer phase
    private boolean nullable = true;

    // Planner-assigned slot id (-1 means not set).
    // Replaces the old AnalyzedSlotRef.desc.getId().asInt() pattern.
    private int slotId = -1;

    // Planner-assigned tuple id (-1 means not set).
    // Replaces the old AnalyzedSlotRef.desc.getParent().getId().asInt() pattern.
    private int tupleId = -1;

    // Only used write
    private SlotRef() {
        super();
    }

    public SlotRef(QualifiedName tblName, String col) {
        super();
        this.tblName = tblName;
        this.colName = col;
        this.label = "`" + col + "`";
    }

    public SlotRef(QualifiedName tblName, String col, String label) {
        super();
        this.tblName = tblName;
        this.colName = col;
        this.label = label;
    }

    public SlotRef(QualifiedName qualifiedName) {
        super(qualifiedName.getPos());
        List<String> parts = qualifiedName.getParts();
        // If parts.size() = 1, it must be a column name. Like `Select a FROM table`.
        // If parts.size() = [2, 3, 4], it maybe a column name or specific struct subfield name.
        checkArgument(parts.size() > 0);
        this.qualifiedName = QualifiedName.of(qualifiedName.getParts(), qualifiedName.getPos());
        if (parts.size() == 1) {
            this.colName = parts.get(0);
            this.label = parts.get(0);
        } else if (parts.size() == 2) {
            this.tblName = QualifiedName.of(List.of(parts.get(0)), qualifiedName.getPos());
            this.colName = parts.get(1);
            this.label = parts.get(1);
        } else if (parts.size() == 3) {
            this.tblName = QualifiedName.of(List.of(parts.get(0), parts.get(1)), qualifiedName.getPos());
            this.colName = parts.get(2);
            this.label = parts.get(2);
        } else if (parts.size() == 4) {
            this.tblName = QualifiedName.of(List.of(parts.get(0), parts.get(1), parts.get(2)),
                    qualifiedName.getPos());
            this.colName = parts.get(3);
            this.label = parts.get(3);
        } else {
            // If parts.size() > 4, it must refer to a struct subfield name, so we set SlotRef's tblName to null,
            // set col, label a qualified name here[Of course it's a wrong value].
            // Correct value will be parsed in Analyzer according context.
            this.tblName = null;
            this.colName = qualifiedName.toString();
            this.label = qualifiedName.toString();
        }
    }

    protected SlotRef(SlotRef other) {
        super(other);
        tblName = other.tblName;
        colName = other.colName;
        columnId = other.columnId;
        label = other.label;
        qualifiedName = other.qualifiedName;
        usedStructFieldPos = other.usedStructFieldPos;
        slotId = other.slotId;
        tupleId = other.tupleId;
    }

    public void setBackQuoted(boolean isBackQuoted) {
        this.isBackQuoted = isBackQuoted;
    }

    public boolean isBackQuoted() {
        return isBackQuoted;
    }

    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public void setUsedStructFieldPos(List<Integer> usedStructFieldPos) {
        this.usedStructFieldPos = ImmutableList.copyOf(usedStructFieldPos);
    }

    public List<Integer> getUsedStructFieldPos() {
        return usedStructFieldPos;
    }

    // When SlotRef is accessing struct subfield, we need to reset SlotRef's type and col name
    // Do this is for compatible with origin SlotRef
    public void resetStructInfo() {
        checkArgument(type.isStructType());
        checkArgument(usedStructFieldPos.size() > 0);

        StringBuilder colStr = new StringBuilder();
        colStr.append(colName);

        setOriginType(type);
        Type tmpType = type;
        for (int pos : usedStructFieldPos) {
            StructField structField = ((StructType) tmpType).getField(pos);
            colStr.append(".");
            colStr.append(structField.getName());
            tmpType = structField.getType();
        }
        // Set type to subfield's type
        type = tmpType;
        // col name like a.b.c
        colName = colStr.toString();
    }

    @Override
    public Expr clone() {
        return new SlotRef(this);
    }

    public boolean isFromLambda() {
        return tblName != null && tblName.getLastPart().equalsIgnoreCase(LAMBDA_FUNC_TABLE);
    }

    public void setTblName(QualifiedName name) {
        this.tblName = name;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getSlotId() {
        return slotId;
    }

    public void setSlotId(int slotId) {
        this.slotId = slotId;
    }

    public boolean hasSlotId() {
        return slotId >= 0;
    }

    public int getTupleId() {
        return tupleId;
    }

    public void setTupleId(int tupleId) {
        this.tupleId = tupleId;
    }

    public boolean hasTupleId() {
        return tupleId >= 0;
    }

    public QualifiedName getTblName() {
        return tblName;
    }

    public String getColName() {
        return colName;
    }

    @Override
    public String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("col", colName);
        helper.add("label", label);
        helper.add("tblName", tblName != null ? tblName.toString() : "null");
        return helper.toString();
    }

    public boolean isColumnRef() {
        return tblName != null && !isFromLambda();
    }

    public QualifiedName getTableName() {
        return tblName;
    }

    @Override
    public int hashCode() {
        if (slotId >= 0) {
            return Integer.hashCode(slotId);
        }
        if (usedStructFieldPos != null) {
            // Means this SlotRef is going to access subfield in StructType
            return Objects.hashCode((tblName == null ? "" : tblName.toString() + "." + label).toLowerCase(),
                    usedStructFieldPos);
        } else {
            return Objects.hashCode((tblName == null ? "" : tblName.toString() + "." + label).toLowerCase());
        }
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        SlotRef other = (SlotRef) obj;
        // If both have slotId set, compare by slotId (same semantics as old AnalyzedSlotRef)
        if (this.slotId >= 0 && other.slotId >= 0) {
            return this.slotId == other.slotId;
        }
        if ((tblName == null) != (other.tblName == null)) {
            return false;
        }
        if (tblName != null && !tblName.equals(other.tblName)) {
            return false;
        }
        if ((colName == null) != (other.colName == null)) {
            return false;
        }
        if (colName != null && !colName.equalsIgnoreCase(other.colName)) {
            return false;
        }

        if (usedStructFieldPos != null && !usedStructFieldPos.equals(other.usedStructFieldPos)) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getColumnName() {
        return colName;
    }

    public void setColumnName(String columnName) {
        this.colName = columnName;
    }

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSlot(this, context);
    }

    public QualifiedName getTblNameWithoutAnalyzed() {
        return tblName;
    }

    @Override
    public boolean isSelfMonotonic() {
        return true;
    }
}

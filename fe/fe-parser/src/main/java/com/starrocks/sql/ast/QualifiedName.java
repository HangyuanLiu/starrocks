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


package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static java.util.Objects.requireNonNull;

/**
 * QualifiedName is used to represent a string connected by "."
 * Often used to represent an unresolved Table Name such as db.table
 */
public class QualifiedName implements ParseNode {
    private final ImmutableList<String> parts;

    private final NodePosition pos;

    public static QualifiedName of(String... originalParts) {
        return of(List.of(originalParts), NodePosition.ZERO);
    }

    public static QualifiedName of(Iterable<String> originalParts) {
        return of(originalParts, NodePosition.ZERO);
    }

    public static QualifiedName of(Iterable<String> originalParts, NodePosition pos) {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        return new QualifiedName(ImmutableList.copyOf(originalParts), pos);
    }

    // Make sure QualifiedName is immutable.
    private QualifiedName(ImmutableList<String> originalParts, NodePosition pos) {
        this.pos = pos;
        this.parts = originalParts;
    }

    public List<String> getParts() {
        return parts;
    }

    /**
     * Returns the last part of the qualified name.
     * For example, for "catalog.db.table", returns "table".
     * For "column", returns "column".
     */
    public String getLastPart() {
        return parts.get(parts.size() - 1);
    }

    /**
     * Returns a SQL representation of this qualified name with backtick-quoted parts.
     * For example, for parts ["db", "table"], returns "`db`.`table`".
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(".");
            }
            sb.append("`").append(parts.get(i)).append("`");
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return Joiner.on('.').join(parts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode() {
        return parts.hashCode();
    }
}




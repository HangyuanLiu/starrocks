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

import com.starrocks.sql.parser.NodePosition;

/**
 * Clause for altering storage volume comment.
 */
public final class AlterStorageVolumeCommentClause extends AlterStorageVolumeClause {
    /**
     * The new comment for the storage volume.
     */
    private final String newComment;

    /**
     * Constructs an AlterStorageVolumeCommentClause.
     *
     * @param comment the new comment for the storage volume
     * @param pos     the position in the source code
     */
    public AlterStorageVolumeCommentClause(final String comment,
                                           final NodePosition pos) {
        super(AlterStorageVolumeClause.AlterOpType.ALTER_COMMENT, pos);
        this.newComment = comment;
    }

    /**
     * Gets the new comment for the storage volume.
     *
     * @return the new comment
     */
    public String getNewComment() {
        return newComment;
    }

    @Override
    public <R, C> R accept(final AstVisitor<R, C> visitor, final final C context) {
        return visitor.visitAlterStorageVolumeCommentClause(this, context);
    }
}

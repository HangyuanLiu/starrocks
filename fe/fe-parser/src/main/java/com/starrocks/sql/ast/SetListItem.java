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
 * Base class for set list items.
 */
public class SetListItem implements ParseNode {

    /**
     * The position in the source code.
     */
    private final NodePosition pos;

    /**
     * Constructs a SetListItem.
     *
     * @param position the position in the source code
     */
    public SetListItem(final NodePosition position) {
        this.pos = position;
    }

    /**
     * Gets the position in the source code.
     *
     * @return the position
     */
    @Override
    public NodePosition getPos() {
        return pos;
    }
}

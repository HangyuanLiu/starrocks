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

package com.starrocks.sql.parser;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.io.Serializable;
import javax.validation.constraints.NotNull;

/**
 * Used to record element position in the sql. ParserRuleContext records the input start and end token,
 * and we can transform their line and col info to NodePosition.
 */
public final class NodePosition implements Serializable {

    /**
     * A zero position constant.
     */
    public static final NodePosition ZERO = new NodePosition(0, 0);

    private static final long serialVersionUID = 5619050719810060066L;

    /**
     * The line number.
     */
    private final int line;

    /**
     * The column number.
     */
    private final int col;

    /**
     * The end line number.
     */
    private final int endLine;

    /**
     * The end column number.
     */
    private final int endCol;

    /**
     * Constructs a NodePosition from a terminal node.
     *
     * @param node the terminal node
     */
    public NodePosition(@NotNull final TerminalNode node) {
        this(node.getSymbol().getLine(), node.getSymbol().getCharPositionInLine());
    }

    /**
     * Constructs a NodePosition from a token.
     *
     * @param token the token
     */
    public NodePosition(@NotNull final Token token) {
        this(token.getLine(), token.getLine());
    }

    /**
     * Constructs a NodePosition from start and end tokens.
     *
     * @param start the start token
     * @param end   the end token
     */
    public NodePosition(@NotNull final Token start, @NotNull final Token end) {
        this(start.getLine(), start.getCharPositionInLine(), 
             end.getLine(), end.getCharPositionInLine());
    }

    /**
     * Constructs a NodePosition with line and column.
     *
     * @param line the line number
     * @param col  the column number
     */
    public NodePosition(final int line, final int col) {
        this(line, col, line, col);
    }

    /**
     * Constructs a NodePosition with line, column, end line, and end column.
     *
     * @param line    the line number
     * @param col     the column number
     * @param endLine the end line number
     * @param endCol  the end column number
     */
    public NodePosition(final int line, final int col, final int endLine, final int endCol) {
        this.line = line;
        this.col = col;
        this.endLine = endLine;
        this.endCol = endCol;
    }

    /**
     * Gets the line number.
     *
     * @return the line number
     */
    public int getLine() {
        return line;
    }

    /**
     * Gets the column number.
     *
     * @return the column number
     */
    public int getCol() {
        return col;
    }

    /**
     * Gets the end line number.
     *
     * @return the end line number
     */
    public int getEndLine() {
        return endLine;
    }

    /**
     * Gets the end column number.
     *
     * @return the end column number
     */
    public int getEndCol() {
        return endCol;
    }

    /**
     * Checks if this position is zero.
     *
     * @return true if all position values are zero
     */
    public boolean isZero() {
        return line == 0 && col == 0 && endLine == 0 && endCol == 0;
    }


}

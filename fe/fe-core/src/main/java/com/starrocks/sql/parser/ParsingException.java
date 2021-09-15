// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import org.antlr.v4.runtime.RecognitionException;

import static java.lang.String.format;

public class ParsingException
        extends RuntimeException {
    private final int line;
    private final int charPositionInLine;

    public ParsingException(String message, RecognitionException cause, int line, int charPositionInLine) {
        super(message, cause);

        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    public ParsingException(String message) {
        this(message, null, 1, 0);
    }

    public int getLineNumber() {
        return line;
    }

    public int getColumnNumber() {
        return charPositionInLine + 1;
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return format("line %s:%s: %s", getLineNumber(), getColumnNumber(), getErrorMessage());
    }
}

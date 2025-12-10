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

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.type.DecimalType;
import com.starrocks.type.DecimalTypeFactory;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;

/**
 * Centralized factory for building {@link LiteralExpr} instances. The logic used to live
 * in {@link LiteralExpr} directly but has been moved here to keep the base class focused
 * on shared literal behaviour.
 */
public final class LiteralExprFactory {
    private LiteralExprFactory() {
    }

    public static LiteralExpr create(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        LiteralExpr literalExpr;
        try {
            switch (type.getPrimitiveType()) {
                case NULL_TYPE:
                    literalExpr = new NullLiteral();
                    break;
                case BOOLEAN:
                    literalExpr = new BoolLiteral(value);
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    literalExpr = new IntLiteral(value, type);
                    break;
                case LARGEINT:
                    literalExpr = new LargeIntLiteral(value);
                    break;
                case FLOAT:
                case DOUBLE:
                    literalExpr = new FloatLiteral(value, type);
                    break;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    literalExpr = createDecimalLiteral(value);
                    break;
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case HLL:
                    literalExpr = new StringLiteral(value);
                    break;
                case DATE:
                case DATETIME:
                    try {
                        LocalDateTime localDateTime = DateUtils.parseStrictDateTime(value);
                        literalExpr = new DateLiteral(localDateTime, type);
                        if (type.isDatetime() && value.contains(".") && localDateTime.getNano() == 0) {
                            int precision = value.length() - value.indexOf(".") - 1;
                            ((DateLiteral) literalExpr).setPrecision(precision);
                        }
                    } catch (Exception ex) {
                        throw new AnalysisException("date literal [" + value + "] is invalid");
                    }
                    break;
                default:
                    throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
            }
        } catch (ParsingException e) {
            throw new AnalysisException(e.getDetailMsg());
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    public static LiteralExpr createInfinity(Type type, boolean isMax) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        if (isMax) {
            return MaxLiteral.MAX_VALUE;
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return IntLiteral.createMinValue(type);
            case LARGEINT:
                return LargeIntLiteral.createMinValue();
            case DATE:
            case DATETIME:
                return DateLiteral.createMinValue(type);
            default:
                throw new AnalysisException("Invalid data type for creating infinity: " + type);
        }
    }

    public static LiteralExpr createDefault(Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        LiteralExpr literalExpr;
        switch (type.getPrimitiveType()) {
            case NULL_TYPE:
                literalExpr = new NullLiteral();
                break;
            case BOOLEAN:
                literalExpr = new BoolLiteral(false);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                literalExpr = new IntLiteral(0, type);
                break;
            case LARGEINT:
                literalExpr = new LargeIntLiteral("0");
                break;
            case FLOAT:
            case DOUBLE:
                literalExpr = new FloatLiteral(0.0, type);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                literalExpr = createDecimalLiteral(BigDecimal.ZERO);
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case HLL:
                literalExpr = new StringLiteral("");
                break;
            case DATE:
            case DATETIME:
                literalExpr = new DateLiteral(DateUtils.parseStrictDateTime("1970-01-01 00:00:00"), type);
                break;
            default:
                throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
        }
        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    private static DecimalLiteral createDecimalLiteral(String value) {
        try {
            return createDecimalLiteral(new BigDecimal(value));
        } catch (NumberFormatException e) {
            throw new ParsingException("Invalid floating-point literal: " + value, e);
        }
    }

    public static DecimalLiteral createDecimalLiteral(BigDecimal bigDecimal) {
        return createDecimalLiteral(bigDecimal, NodePosition.ZERO);
    }

    public static DecimalLiteral createDecimalLiteral(BigDecimal bigDecimal, NodePosition pos) {
        // Currently, our storage engine doesn't support scientific notation.
        // So we remove exponent field here.
        BigDecimal value = new BigDecimal(bigDecimal.toPlainString());

        Type type;
        if (!Config.enable_decimal_v3) {
            type = DecimalType.DECIMALV2;
        } else {
            int precision = getRealPrecision(value);
            int scale = getRealScale(value);
            int integerPartWidth = precision - scale;
            int maxIntegerPartWidth = 76;
            // integer part of decimal literal should not exceed 76
            if (integerPartWidth > maxIntegerPartWidth) {
                String errMsg = String.format(
                        "Non-typed decimal literal is overflow, value='%s' (precision=%d, scale=%d)",
                        bigDecimal.toPlainString(), precision, scale);
                throw new InternalError(errMsg);
            }
            // round to low-resolution decimal if decimal literal's resolution is too high
            scale = Math.min(maxIntegerPartWidth - integerPartWidth, scale);
            precision = integerPartWidth + scale;
            value = value.setScale(scale, RoundingMode.HALF_UP);
            type = DecimalTypeFactory.createDecimalV3NarrowestType(precision, scale);
        }

        return new DecimalLiteral(value, type, pos);
    }

    // Precision and scale of BigDecimal is subtly different from Decimal32/64/128/256.
    // In BigDecimal, the precision is the number of digits in the unscaled BigInteger value.
    // there are two types of subnormal BigDecimal violate the invariants:  0 < P and 0 <= S <= P
    // type 1: 0 <= S but S > P.  i.e. BigDecimal("0.0001"), unscaled BigInteger is 1, the scale is 4
    // type 2: S < 0. i.e. BigDecimal("10000"), unscaled BigInteger is 1, the  scaled is -4
    public static int getRealPrecision(BigDecimal decimal) {
        if (decimal.equals(BigDecimal.ZERO)) {
            return 0;
        }
        int scale = decimal.scale();
        int precision = decimal.precision();
        if (scale < 0) {
            return Math.abs(scale) + precision;
        } else {
            return Math.max(scale, precision);
        }
    }

    // An integer that has trailing zeros represented by BigDecimal with negative scale, i.e.
    // BigDecimal("20000"):  unscaled integer is 2, the scale is -5.
    public static int getRealScale(BigDecimal decimal) {
        if (decimal.equals(BigDecimal.ZERO)) {
            return 0;
        }
        return Math.max(0, decimal.scale());
    }
}

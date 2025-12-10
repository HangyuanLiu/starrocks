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

package com.starrocks.type;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;

public class DecimalTypeFactory {
    /**
     * Create a unified decimal type with specified precision and scale.
     * Automatically chooses between DecimalV2 and DecimalV3 based on configuration.
     *
     * @param precision the precision
     * @param scale     the scale
     * @return the created decimal type
     */
    public static ScalarType createUnifiedDecimalType(int precision, int scale) {
        ConnectContext ctx = ConnectContext.get();
        String underlyingType = SessionVariableConstants.PANIC;
        if (ctx != null) {
            underlyingType = ctx.getSessionVariable().getLargeDecimalUnderlyingType();
        }

        return TypeFactory.createUnifiedDecimalType(precision, scale, underlyingType);
    }

    /**
     * Create a DecimalV3 type with specified primitive type, precision and scale.
     *
     * @param type      the decimal primitive type (DECIMAL32/64/128/256)
     * @param precision the precision
     * @param scale     the scale
     * @return the created DecimalV3 type
     */
    public static ScalarType createDecimalV3Type(PrimitiveType type, int precision, int scale) {
        ConnectContext ctx = ConnectContext.get();
        String underlyingType = SessionVariableConstants.PANIC;
        if (ctx != null) {
            underlyingType = ctx.getSessionVariable().getLargeDecimalUnderlyingType();
        }

        return TypeFactory.createDecimalV3Type(type, precision, scale, underlyingType);
    }

    /**
     * Create the narrowest DecimalV3 type that can hold the specified precision and scale.
     * Chooses the smallest DecimalV3 type (DECIMAL32 -> DECIMAL64 -> DECIMAL128 -> DECIMAL256)
     * that can accommodate the given precision.
     *
     * @param precision the precision
     * @param scale     the scale
     * @return the created narrowest DecimalV3 type
     */
    public static ScalarType createDecimalV3NarrowestType(int precision, int scale) {
        ConnectContext ctx = ConnectContext.get();
        String underlyingType = SessionVariableConstants.PANIC;
        if (ctx != null) {
            underlyingType = ctx.getSessionVariable().getLargeDecimalUnderlyingType();
        }

        return TypeFactory.createDecimalV3NarrowestType(precision, scale, underlyingType);
    }
}

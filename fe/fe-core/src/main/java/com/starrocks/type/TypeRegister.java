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

import com.starrocks.common.Config;

public class TypeRegister {
    static {
        //initializeSpecialTypes();
    }

    /**
     * Initialize the special ScalarType instances that depend on configuration.
     * Calling this method explicitly ensures the static initialization happens.
     */
    public static void initializeSpecialTypes() {
        TypeOptions.configureStringMaxLength(Config.max_varchar_length);
        TypeOptions.configureDecimalV3Enabled(Config.enable_decimal_v3);
    }

    /**
     * Get the maximum varchar length for OLAP tables.
     *
     * @return the maximum varchar length
     */
    public static int getOlapMaxVarcharLength() {
        return Config.max_varchar_length;
    }
}

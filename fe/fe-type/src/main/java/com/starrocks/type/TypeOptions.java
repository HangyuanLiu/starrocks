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

/**
 * Holds configuration values propagated from FE core into the shared type module.
 * This avoids leaking {@code Config} or other FE-specific dependencies here.
 */
final class TypeOptions {
    private static volatile int maxStringLength = StringType.MAX_STRING_LENGTH;
    private static volatile boolean decimalV3Enabled = true;

    private TypeOptions() {
    }

    static void configureStringMaxLength(int length) {
        maxStringLength = length;
    }

    static int getStringMaxLength() {
        return maxStringLength;
    }

    static void configureDecimalV3Enabled(boolean enable) {
        decimalV3Enabled = enable;
    }

    static boolean isDecimalV3Enabled() {
        return decimalV3Enabled;
    }
}

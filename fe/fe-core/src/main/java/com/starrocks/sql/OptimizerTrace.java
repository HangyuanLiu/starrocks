// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql;

import org.apache.commons.lang3.time.StopWatch;

public class OptimizerTrace {
    StopWatch watch;
    public OptimizerTrace(String name) {
        watch.start(name);
    }
}

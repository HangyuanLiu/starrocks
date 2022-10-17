// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql;

import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OptimizerTrace {
    private final String name;
    private final Stopwatch watch;
    private final List<OptimizerTrace> children;

    public OptimizerTrace(String name) {
        this.name = name;
        watch = Stopwatch.createStarted();
        children = new ArrayList<>();
    }

    public void addChild(OptimizerTrace trace) {
        this.children.add(trace);
    }

    public void stop() {
        watch.stop();
        children.forEach(OptimizerTrace::stop);
    }

    public void print(int step) {
        System.out.println(
                String.join("", Collections.nCopies(step, "    "))
                        + "-- "
                        + name
                        + " "
                        + watch.elapsed(TimeUnit.MILLISECONDS)
                        + "ms"
        );
        children.forEach(c -> c.print(step + 1));
    }
}

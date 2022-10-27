// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql;

import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    private static Long getTime(String prefix, Map<String, PlannerProfile.ScopedTimer> times) {
        if (times.containsKey(prefix)) {
            return times.get(prefix).getTotalTime();
        } else {
            return 0L;
        }
    }

    public static String print(String name, long time, int step) {
        return String.join("", Collections.nCopies(step, "    ")) + "-- " + name + " " + time + "ms" + "\n";
    }

    public static String print(String name, Map<String, Long> times, int step) {
        long total = 0;
        for (Map.Entry<String, Long> t : times.entrySet()) {
            total += t.getValue();
        }
        return print(name, total, step);
    }

    public static String explain(PlannerProfile profile) {
        StringBuilder trace = new StringBuilder();
        Map<String, PlannerProfile.ScopedTimer> times = profile.getTimers();

        trace.append(print("Total", getTime("Total", times), 0));
        trace.append(print("Parser", getTime("Parser", times), 1));
        trace.append(print("Analyzer", getTime("Analyzer", times), 1));
        trace.append(print("Optimizer", getTime("Optimizer", times), 1));
        trace.append(print("Optimizer.RuleBaseOptimize",
                getTime("Optimizer.RuleBaseOptimize", times), 2));
        trace.append(print("Optimizer.CostBaseOptimize",
                getTime("Optimizer.CostBaseOptimize", times), 2));
        trace.append(print("Optimizer.PhysicalRewrite",
                getTime("Optimizer.PhysicalRewrite", times), 2));
        trace.append(print("ExecPlanBuild", getTime("ExecPlanBuild", times), 1));
        trace.append(print("ExecPlanBuild.addScanRangeLocations",
                getTime("ExecPlanBuild.addScanRangeLocations", times), 2));

        return trace.toString();
    }
}

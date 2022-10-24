// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql;

import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

    private static Map<String, Long> getPrefix(String prefix, Map<String, PlannerProfile.ScopedTimer> times) {
        Map<String, Long> prefixScopeTimes = new HashMap<>();
        for (Map.Entry<String, PlannerProfile.ScopedTimer> entry : times.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                prefixScopeTimes.put(entry.getKey(), entry.getValue().getTotalTime());
            }
        }
        return prefixScopeTimes;
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
        long total = 0;
        for (PlannerProfile.ScopedTimer scopedTimer : times.values()) {
            total += scopedTimer.getTotalTime();
        }
        trace.append(print("Total", total, 0));

        Map<String, Long> parser = getPrefix("Parser", times);
        trace.append(print("Parser", parser, 1));

        Map<String, Long> analyzer = getPrefix("Analyzer", times);
        trace.append(print("Analyzer", analyzer, 1));


        Map<String, Long> optimizer = getPrefix("Optimizer", times);
        trace.append(print("Optimizer", optimizer, 1));

        Map<String, Long> rbo = getPrefix("Optimizer.RuleBaseOptimize", times);
        trace.append(print("Optimizer.RuleBaseOptimize", rbo, 2));
        Map<String, Long> cbo = getPrefix("Optimizer.CostBaseOptimize", times);
        trace.append(print("Optimizer.CostBaseOptimize", cbo, 2));
        Map<String, Long> rewrite = getPrefix("Optimizer.PhysicalRewrite", times);
        trace.append(print("Optimizer.PhysicalRewrite", rewrite, 2));

        Map<String, Long> exec = getPrefix("ExecPlanBuild", times);
        trace.append(print("ExecPlanBuild", exec, 1));

        Map<String, Long> t1 = getPrefix("SelectPartition", times);
        trace.append(print("SelectPartition", t1, 1));

        Map<String, Long> t2 = getPrefix("addScanRangeLocations", times);
        trace.append(print("addScanRangeLocations", t2, 1));

        return trace.toString();
    }
}

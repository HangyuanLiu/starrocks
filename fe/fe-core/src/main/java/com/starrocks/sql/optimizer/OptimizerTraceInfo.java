// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Stopwatch;
import com.starrocks.common.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// OptimizerTraceInfo is used to record some important info during query optimization
public class OptimizerTraceInfo {
    private final UUID queryId;
    private final Map<String, Integer> rulesAppliedTimes = new HashMap<>();
    private final Stopwatch stopwatch;
    private final List<Pair<String, Long>> rulesTimeCost = new ArrayList<>();


    public OptimizerTraceInfo(UUID queryId) {
        this.queryId = queryId;
        this.stopwatch = Stopwatch.createStarted();
    }

    public void recordAppliedRule(String rule) {
        rulesAppliedTimes.merge(rule, 1, Integer::sum);
    }

    public Map<String, Integer> getRulesAppliedTimes() {
        return rulesAppliedTimes;
    }

    public void recordRuleTimeCost(String rule, Long eclipse) {
        rulesTimeCost.add(new Pair<>(rule, eclipse));
    }

    public List<Pair<String, Long>>  getRulesTimeCost() {
        return rulesTimeCost;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public Stopwatch getStopwatch() {
        return stopwatch;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OptimizerTraceInfo");
        sb.append("\nRules' applied times\n").append(rulesAppliedTimes);
        return sb.toString();
    }
}

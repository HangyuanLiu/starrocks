// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.plan;

import com.google.common.base.Stopwatch;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExecPlanTest {
    @Test
    public void test() throws Exception {
        Stopwatch watch = Stopwatch.createUnstarted();
        watch.start();

        //List<TScanRangeLocations> result = new LinkedList();
        //List<TScanRangeLocations> result = new ArrayList<>(2000000);
        List<TScanRangeLocations> result = new ArrayList<>();
        for (int i = 0; i < 1500000; ++i) {
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            TInternalScanRange internalRange = new TInternalScanRange();
            TScanRange scanRange = new TScanRange();
            scanRange.setInternal_scan_range(internalRange);
            scanRangeLocations.setScan_range(scanRange);

            result.add(scanRangeLocations);
        }
        System.out.println(result.size());

        watch.stop();
        System.out.println(watch.elapsed(TimeUnit.MILLISECONDS));
    }
}

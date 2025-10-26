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

package com.starrocks.connector.partitiontraits;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.starrocks.StarRocksExternalTable;

import java.util.List;
import java.util.stream.Collectors;

public class StarRocksPartitionTraits extends DefaultTraits {

    @Override
    public boolean isSupportPCTRefresh() {
        return false;
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new PartitionKey();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        // Treat as unpartitioned, return modified time of the table
        long modifiedTime = 0;
        if (table instanceof StarRocksExternalTable) {
            modifiedTime = ((StarRocksExternalTable) table).getRefreshedTimestamp();
        }
        long finalModifiedTime = modifiedTime;
        return partitionNames.stream()
                .map(name -> new StarRocksPartitionInfo(finalModifiedTime))
                .collect(Collectors.toList());
    }

    private static class StarRocksPartitionInfo implements PartitionInfo {
        private final long modifiedTime;

        public StarRocksPartitionInfo(long modifiedTime) {
            this.modifiedTime = modifiedTime;
        }

        @Override
        public long getModifiedTime() {
            return modifiedTime;
        }
    }
}

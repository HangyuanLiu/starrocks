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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.lake.LakeTablet;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks tablets that should be forcefully deleted on specific backends.
 * This state is only used to set force flag for DropReplicaTask and does not
 * need to be guarded by TabletInvertedIndex's lock.
 */
public class ForceDeleteTracker {
    private final ConcurrentMap<Long, Set<Long>> forceDeleteTablets = new ConcurrentHashMap<>();

    public boolean contains(long tabletId, long backendId) {
        Set<Long> backendIds = forceDeleteTablets.get(tabletId);
        return backendIds != null && backendIds.contains(backendId);
    }

    public void mark(long tabletId, long backendId) {
        forceDeleteTablets.compute(tabletId, (key, backendIds) -> {
            Set<Long> backendSet = backendIds;
            if (backendSet == null) {
                backendSet = ConcurrentHashMap.newKeySet();
            }
            backendSet.add(backendId);
            return backendSet;
        });
    }

    public void mark(long tabletId, Set<Long> backendIds) {
        if (backendIds.isEmpty()) {
            return;
        }
        forceDeleteTablets.compute(tabletId, (key, existing) -> {
            Set<Long> backendSet = existing;
            if (backendSet == null) {
                backendSet = ConcurrentHashMap.newKeySet();
            }
            backendSet.addAll(backendIds);
            return backendSet;
        });
    }

    public void mark(Tablet tablet) {
        if (tablet instanceof LakeTablet) {
            return;
        }
        mark(tablet.getId(), tablet.getBackendIds());
    }

    public void erase(long tabletId, long backendId) {
        forceDeleteTablets.computeIfPresent(tabletId, (key, backendIds) -> {
            backendIds.remove(backendId);
            return backendIds.isEmpty() ? null : backendIds;
        });
    }

    public void eraseTablet(long tabletId) {
        forceDeleteTablets.remove(tabletId);
    }

    public void clear() {
        forceDeleteTablets.clear();
    }

    public ImmutableMap<Long, Set<Long>> snapshot() {
        ImmutableMap.Builder<Long, Set<Long>> builder = ImmutableMap.builder();
        forceDeleteTablets.forEach((tabletId, backendIds) ->
                builder.put(tabletId, ImmutableSet.copyOf(backendIds)));
        return builder.build();
    }
}

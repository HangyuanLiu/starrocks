// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

// Homebrew's jemalloc installs "unprefixed" symbols (e.g. mallctl, malloc_stats_print)
// and the header intentionally undefines the je_* rename macros after it is included.
// StarRocks uses je_* names in several files; force-include this shim so we can map
// je_* APIs to the corresponding unprefixed ones on macOS without modifying core sources.

#ifdef __APPLE__

#include <jemalloc/jemalloc.h>

#ifndef je_malloc_stats_print
#define je_malloc_stats_print malloc_stats_print
#endif

#ifndef je_mallctl
#define je_mallctl mallctl
#endif

#ifndef je_mallctlnametomib
#define je_mallctlnametomib mallctlnametomib
#endif

#ifndef je_mallctlbymib
#define je_mallctlbymib mallctlbymib
#endif

#ifndef je_malloc_usable_size
#define je_malloc_usable_size malloc_usable_size
#endif

#ifndef je_nallocx
#define je_nallocx nallocx
#endif

#endif // __APPLE__


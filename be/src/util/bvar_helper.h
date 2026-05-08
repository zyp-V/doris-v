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
#include <bvar/latency_recorder.h>
#include <gflags/gflags_declare.h>

#include "common/config.h"
#include "defer_op.h"
#include "stopwatch.hpp"

namespace bvar {
DECLARE_int32(bvar_dump_interval);
} // namespace bvar

namespace doris {

// Apply Doris configs to bvar flags.
inline void init_bvar_flags_from_config() {
    bvar::FLAGS_bvar_dump_interval = doris::config::bvar_dump_interval;
}

#define SCOPED_BVAR_LATENCY(bvar_item) \
    MonotonicStopWatch __watch;        \
    __watch.start();                   \
    Defer __record_bvar([&] { bvar_item << __watch.elapsed_time() / 1000; });

} // end namespace doris

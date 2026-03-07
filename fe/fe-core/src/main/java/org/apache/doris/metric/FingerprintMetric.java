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

package org.apache.doris.metric;

import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.thrift.TQueryStatistics;

public class FingerprintMetric {
    public static void reportFingerprint(AuditEvent auditEvent, TQueryStatistics queryStats) {
        if (queryStats == null || (auditEvent.fingerprint == null || auditEvent.fingerprint.isEmpty())) {
            return;
        }
        long scanRows = queryStats.scan_rows;
        long scanBytes = queryStats.scan_bytes;
        long memoryUsageBytes = queryStats.max_peak_memory_bytes;
        long cpuTimeMS = queryStats.cpu_ms;
        long sendBytes = queryStats.shuffle_send_bytes;
        long sendRows = queryStats.shuffle_send_rows;

        String countMetricName = "_count";
        String scanBytesMetricName = "_scanbytes";
        String scanRowsMetricName = "_scanrows";
        String memoryMetricName = "_memory";
        String cpuTimeMetricName = "_cpu_time";
        String shuffleSendBytes = "_shuffle_send_bytes";
        String shuffleSendRows = "_shuffle_send_rows";
        if (auditEvent.isInsert) {
            countMetricName = "_insert_count";
            scanBytesMetricName = "_insert_scanbytes";
            scanRowsMetricName = "_insert_scanrows";
            memoryMetricName = "_insert_memory";
            cpuTimeMetricName = "_insert_cpu_time";
            shuffleSendBytes = "_insert_shuffle_send_bytes";
            shuffleSendRows = "_insert_shuffle_send_rows";
        }
        String shortFingerprint = auditEvent.fingerprint.substring(0, 12);
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + countMetricName, 1L,
                "fingerprint" + countMetricName, Metric.MetricUnit.REQUESTS,
                "query count for every fingerprint");
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + scanBytesMetricName, scanBytes,
                "fingerprint" + scanBytesMetricName, Metric.MetricUnit.BYTES,
                "scanbytes for every fingerprint");
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + scanRowsMetricName, scanRows,
                "fingerprint" + scanRowsMetricName, Metric.MetricUnit.BYTES,
                "scanrows for every fingerprint");
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + memoryMetricName, memoryUsageBytes,
                "fingerprint" + memoryMetricName, Metric.MetricUnit.BYTES,
                "peak memory usage for fingerprint");
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + cpuTimeMetricName, cpuTimeMS,
                "fingerprint" + cpuTimeMetricName, Metric.MetricUnit.MICROSECONDS,
                "cpu time for fingerprint");
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + shuffleSendBytes,
                sendBytes, "fingerprint" + shuffleSendBytes,
                Metric.MetricUnit.BYTES, "shuffle send bytes per fingerprint");
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + shuffleSendRows,
                sendRows, "fingerprint" + shuffleSendRows,
                Metric.MetricUnit.BYTES, "shuffle send rows per fingerprint");
        MetricRepo.addFingerprintQueryLatency(shortFingerprint, auditEvent.queryTime);
    }

    public static void reportPointQueryFingerprint(AuditEvent auditEvent) {
        String countMetricName = "_count";
        String shortFingerprint = auditEvent.fingerprint.substring(0, 12);
        MetricRepo.addFingerprint(shortFingerprint, shortFingerprint + countMetricName, 1L,
                "fingerprint" + countMetricName, Metric.MetricUnit.REQUESTS,
                "query count for every fingerprint");
        MetricRepo.addFingerprintQueryLatency(shortFingerprint, auditEvent.queryTime);
    }
}

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

package org.apache.doris.catalog.stream;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotOutOfRangeException;
import org.apache.doris.thrift.TRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class PaimonTableStreamWrapperTest {

    @Test
    public void testResolveSnapshotOffsetBySnapshotId() throws Exception {
        PaimonSnapshot snapshot = PaimonTableStreamWrapper.resolveSnapshotOffsetFromSnapshots(
                buildOffsets(), "2", PaimonStreamOffsetType.SNAPSHOT_ID);

        Assertions.assertEquals(2L, snapshot.getSnapshotId());
        Assertions.assertEquals(2000L, snapshot.getCommitTimestampMs());
    }

    @Test
    public void testResolveSnapshotOffsetByTimestamp() throws Exception {
        PaimonSnapshot snapshot = PaimonTableStreamWrapper.resolveSnapshotOffsetFromSnapshots(
                buildOffsets(), "1500", PaimonStreamOffsetType.TIMESTAMP);

        Assertions.assertEquals(2L, snapshot.getSnapshotId());
        Assertions.assertEquals(2000L, snapshot.getCommitTimestampMs());
    }

    @Test
    public void testResolveSnapshotOffsetOutOfRange() {
        Assertions.assertThrows(PaimonSnapshotOutOfRangeException.class,
                () -> PaimonTableStreamWrapper.resolveSnapshotOffsetFromSnapshots(
                        buildOffsets(), "9", PaimonStreamOffsetType.SNAPSHOT_ID));
        Assertions.assertThrows(PaimonSnapshotOutOfRangeException.class,
                () -> PaimonTableStreamWrapper.resolveSnapshotOffsetFromSnapshots(
                        buildOffsets(), "500", PaimonStreamOffsetType.TIMESTAMP));
        Assertions.assertThrows(PaimonSnapshotOutOfRangeException.class,
                () -> PaimonTableStreamWrapper.resolveSnapshotOffsetFromSnapshots(
                        buildOffsets(), "4000", PaimonStreamOffsetType.TIMESTAMP));
    }

    @Test
    public void testPaimonStreamOffsetTypeFromString() throws Exception {
        Assertions.assertEquals(PaimonStreamOffsetType.SNAPSHOT_ID, PaimonStreamOffsetType.fromString(null));
        Assertions.assertEquals(PaimonStreamOffsetType.SNAPSHOT_ID,
                PaimonStreamOffsetType.fromString("snapshot_id"));
        Assertions.assertEquals(PaimonStreamOffsetType.TIMESTAMP,
                PaimonStreamOffsetType.fromString("timestamp"));
        Assertions.assertThrows(AnalysisException.class, () -> PaimonStreamOffsetType.fromString("time"));
    }

    @Test
    public void testBuildFullReadScanParams() {
        TableScanParams params = PaimonTableStreamWrapper.buildFullReadScanParams(10L, 1234L);

        Assertions.assertTrue(params.fullRead());
        Assertions.assertEquals("10", params.getParams().get(PaimonTableStreamWrapper.FULL_SNAPSHOT_ID_PARAM));
        Assertions.assertEquals("10", params.getParams().get(PaimonTableStreamWrapper.END_SNAPSHOT_ID_PARAM));
        Assertions.assertEquals("1234",
                params.getParams().get(PaimonTableStreamWrapper.END_COMMIT_TIMESTAMP_MS_PARAM));
    }

    @Test
    public void testBuildEmptyIncrementalReadScanParams() {
        TableScanParams params = PaimonTableStreamWrapper.buildEmptyIncrementalReadScanParams(1234L);

        Assertions.assertTrue(params.incrementalRead());
        Assertions.assertTrue(PaimonTableStreamWrapper.isEmptyReadScan(params));
        Assertions.assertEquals("1234",
                params.getParams().get(PaimonTableStreamWrapper.END_COMMIT_TIMESTAMP_MS_PARAM));
    }

    @Test
    public void testRejectUserIncrementalScanParamsOnPaimonStream() {
        Assertions.assertThrows(UserException.class,
                () -> PaimonTableStreamWrapper.validateUserScanParams(
                        new TableScanParams(TableScanParams.INCREMENTAL_READ,
                                Collections.emptyMap())));
        Assertions.assertDoesNotThrow(() -> PaimonTableStreamWrapper.validateUserScanParams(
                new TableScanParams(TableScanParams.FULL_READ, Collections.emptyMap())));
    }

    @Test
    public void testNormalizeMinDeltaStartSnapshot() {
        Assertions.assertEquals(1L, PaimonTableStreamWrapper.normalizeStartSnapshotId(
                BaseTableStream.StreamConsumeType.MIN_DELTA, 0L, 3L));
        Assertions.assertEquals(0L, PaimonTableStreamWrapper.normalizeStartSnapshotId(
                BaseTableStream.StreamConsumeType.APPEND_ONLY, 0L, 3L));
    }

    @Test
    public void testPaimonTableStreamCommittedOffset() {
        PaimonTableStream stream = new PaimonTableStream();
        stream.setSnapshotOffset(0L, 1000L, 3L);

        Assertions.assertEquals(0L, stream.getConsumedSnapshotId());
        Assertions.assertEquals(1000L, stream.getConsumedCommitTimestampMs());
        stream.setCommittedSnapshotOffset(3L, stream.getConsumedCommitTimestampMs());
        Assertions.assertEquals(3L, stream.getSnapshotId());
        Assertions.assertEquals(PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID, stream.getHistoricalSnapshotId());
    }

    @Test
    public void testPaimonTableStreamConsumptionRowsExposeOffsetTypeAndLagMetrics() {
        PaimonTableStream stream = new PaimonTableStream();
        stream.setCommittedSnapshotOffset(7L, 8000L);

        List<TRow> rows = new ArrayList<>();
        stream.fillTableStreamConsumptionInfo(rows);

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("N/A", rows.get(0).getColumnValue().get(3).getStringVal());
        Assertions.assertEquals("Paimon Snapshot ID", rows.get(0).getColumnValue().get(4).getStringVal());
        Assertions.assertEquals("7", rows.get(0).getColumnValue().get(5).getStringVal());
        Assertions.assertEquals("-1", rows.get(0).getColumnValue().get(6).getStringVal());
        Assertions.assertEquals(8000L, rows.get(0).getColumnValue().get(7).getLongVal());
        Assertions.assertEquals("N/A", rows.get(1).getColumnValue().get(3).getStringVal());
        Assertions.assertEquals("Paimon Snapshot Commit Time", rows.get(1).getColumnValue().get(4).getStringVal());
        Assertions.assertEquals("8000", rows.get(1).getColumnValue().get(5).getStringVal());
        Assertions.assertEquals("-1", rows.get(1).getColumnValue().get(6).getStringVal());
        Assertions.assertEquals(8000L, rows.get(1).getColumnValue().get(7).getLongVal());
    }

    @Test
    public void testPaimonTableStreamUsesObjectIdentity() {
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(100L);
        Mockito.when(catalog.getName()).thenReturn("paimon_catalog");

        PaimonTableStream stream1 = buildPaimonTableStream(catalog, 1L, "stream_1", "stream_db_1",
                21L, "base_db_1", 31L, "base_table_1");
        PaimonTableStream stream2 = buildPaimonTableStream(catalog, 2L, "stream_2", "stream_db_2",
                22L, "base_db_2", 32L, "base_table_2");
        stream1.setSnapshotOffset(7L, 8000L, PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID);
        stream2.setSnapshotOffset(7L, 8000L, PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID);

        Assertions.assertNotEquals(stream1, stream2);
        HashSet<PaimonTableStream> streams = new HashSet<>();
        streams.add(stream1);
        streams.add(stream2);
        Assertions.assertEquals(2, streams.size());
    }

    private List<PaimonSnapshot> buildOffsets() {
        return Arrays.asList(
                new PaimonSnapshot(1L, 0L, 1000L),
                new PaimonSnapshot(2L, 0L, 2000L),
                new PaimonSnapshot(3L, 0L, 3000L));
    }

    private PaimonTableStream buildPaimonTableStream(PaimonExternalCatalog catalog, long streamId,
            String streamName, String streamDbName, long baseDbId, String baseDbName, long baseTableId,
            String baseTableName) {
        PaimonExternalDatabase baseDb = new PaimonExternalDatabase(catalog, baseDbId, baseDbName, baseDbName);
        PaimonExternalTable baseTable = new PaimonExternalTable(baseTableId, baseTableName, baseTableName, catalog,
                baseDb);
        PaimonTableStream stream = new PaimonTableStream(streamId, streamName, Collections.emptyList(), baseTable);
        stream.setQualifiedDbName(streamDbName);
        return stream;
    }
}

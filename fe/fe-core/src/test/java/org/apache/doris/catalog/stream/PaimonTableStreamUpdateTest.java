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

import org.apache.doris.persist.AlterStreamOffsetOperationLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.transaction.TransactionCommitFailedException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PaimonTableStreamUpdateTest {

    @Test
    public void testCheckSnapshotOffsetForIncrementalConsumeOk() throws Exception {
        PaimonTableStreamUpdate update = new PaimonTableStreamUpdate(10L, 12L, 1000L);

        update.checkSnapshotOffset("test_db", "s1", 10L,
                PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID);
    }

    @Test
    public void testCheckSnapshotOffsetForIncrementalConsumeConflict() {
        PaimonTableStreamUpdate update = new PaimonTableStreamUpdate(10L, 12L, 1000L);

        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> update.checkSnapshotOffset("test_db", "s1", 11L,
                        PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID));
        Assertions.assertTrue(exception.getMessage().contains("target offset already consumed"));
    }

    @Test
    public void testCheckSnapshotOffsetForHistoricalConsumeOk() throws Exception {
        PaimonTableStreamUpdate update = new PaimonTableStreamUpdate(0L, 5L, 1000L);

        update.checkSnapshotOffset("test_db", "s1", 0L, 5L);
    }

    @Test
    public void testCheckSnapshotOffsetForExplicitFullReadWithHistoricalMarkerOk() throws Exception {
        PaimonTableStreamUpdate update = new PaimonTableStreamUpdate(0L, 8L, 2000L);

        update.checkSnapshotOffset("test_db", "s1", 0L, 5L);
    }

    @Test
    public void testCheckSnapshotOffsetForHistoricalConsumeConflict() {
        PaimonTableStreamUpdate update = new PaimonTableStreamUpdate(0L, 4L, 1000L);

        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> update.checkSnapshotOffset("test_db", "s1", 0L, 5L));
        Assertions.assertTrue(exception.getMessage().contains("history offset already consumed"));
    }

    @Test
    public void testPaimonStreamUpdateInfoGsonSerde() {
        TableStreamUpdateInfo info = new TableStreamUpdateInfo(1L, 2L,
                new PaimonTableStreamUpdate(3L, 4L, 5L));

        TableStreamUpdateInfo decoded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info),
                TableStreamUpdateInfo.class);

        Assertions.assertEquals(1L, decoded.getDbId());
        Assertions.assertEquals(2L, decoded.getStreamId());
        Assertions.assertTrue(decoded.getUpdate() instanceof PaimonTableStreamUpdate);
        PaimonTableStreamUpdate decodedUpdate = (PaimonTableStreamUpdate) decoded.getUpdate();
        Assertions.assertEquals(3L, decodedUpdate.getPrevSnapshotId());
        Assertions.assertEquals(4L, decodedUpdate.getNextSnapshotId());
        Assertions.assertEquals(5L, decodedUpdate.getNextCommitTimestampMs());
    }

    @Test
    public void testAlterStreamOffsetOperationLogGsonSerde() {
        AlterStreamOffsetOperationLog log = new AlterStreamOffsetOperationLog(1L, 2L, "s1", 3L, 4L,
                PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID);

        AlterStreamOffsetOperationLog decoded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log),
                AlterStreamOffsetOperationLog.class);

        Assertions.assertEquals(1L, decoded.getDbId());
        Assertions.assertEquals(2L, decoded.getStreamId());
        Assertions.assertEquals("s1", decoded.getStreamName());
        Assertions.assertEquals(3L, decoded.getSnapshotId());
        Assertions.assertEquals(4L, decoded.getCommitTimestampMs());
        Assertions.assertEquals(PaimonTableStream.INVALID_HISTORICAL_SNAPSHOT_ID,
                decoded.getHistoricalSnapshotId());
    }
}

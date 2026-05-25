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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableStreamOffsetTransactionTest extends TestWithFeService {

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;

        createDatabase("test_stream");
        connectContext.setDatabase("test_stream");

        String createBaseTable = "create table test_stream.tbl_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createBaseTable);

        String createTargetTable = "create table test_stream.tbl_target (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createTargetTable);

        String createStream = "create stream if not exists test_stream.s1 on table test_stream.tbl_stream_base\n"
                + "properties('type' = 'default', 'show_initial_rows' = 'true')";
        createTable(createStream, true);

        createDatabase("test_stream_target");
        connectContext.setDatabase("test_stream_target");
        String createCrossDbTargetTable = "create table test_stream_target.tbl_target_cross_db (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createCrossDbTargetTable);
        connectContext.setDatabase("test_stream");
    }

    @Test
    public void testHistoricalConsumeOffsetCheckAndUpdate() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> partitionIds = new ArrayList<>(baseTable.getPartitionIds());
        Map<Long, Long> historicalPartitionOffset = new HashMap<>();
        Map<Long, Long> partitionOffset = new HashMap<>();
        for (Long partitionId : partitionIds) {
            historicalPartitionOffset.put(partitionId, 100L);
            partitionOffset.put(partitionId, 0L);
        }
        Deencapsulation.setField(stream, "historicalPartitionOffset", historicalPartitionOffset);
        Deencapsulation.setField(stream, "partitionOffset", partitionOffset);

        OlapTableStreamUpdate update = new OlapTableStreamUpdate(new HashMap<>(),
                new HashMap<>(historicalPartitionOffset));
        Assertions.assertTrue(update.getNext().keySet().containsAll(partitionIds));

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
        long txnId = -1L;
        try {
            txnId = transactionMgr.beginTransaction(db.getId(), Collections.singletonList(targetTable.getId()),
                    "ut_table_stream_hist_ok_" + System.nanoTime(), coordinator,
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

            TransactionState transactionState = transactionMgr.getTransactionState(db.getId(), txnId);
            transactionState.setStreamUpdateInfos(Collections.singletonList(
                    new TableStreamUpdateInfo(db.getId(), stream.getId(), update)));

            DatabaseTransactionMgr dbTxnMgr = transactionMgr.getDatabaseTransactionMgr(db.getId());

            long commitTime = System.currentTimeMillis();
            stream.writeLock();
            try {
                Deencapsulation.invoke(dbTxnMgr, "checkStreamOffset", transactionState);
                Deencapsulation.invoke(dbTxnMgr, "updateStreamOffset", transactionState, commitTime);
            } finally {
                stream.writeUnlock();
            }

            Map<Long, Long> updatedHistoricalPartitionOffset = Deencapsulation.getField(stream,
                    "historicalPartitionOffset");
            Map<Long, Long> updatedPartitionOffset = Deencapsulation.getField(stream, "partitionOffset");
            Map<Long, Long> partitionConsumptionTime = Deencapsulation.getField(stream, "partitionConsumptionTime");

            for (Long pid : partitionIds) {
                Assertions.assertFalse(updatedHistoricalPartitionOffset.containsKey(pid));
                Assertions.assertEquals(update.getNext().get(pid), updatedPartitionOffset.get(pid));
                Assertions.assertEquals(commitTime, partitionConsumptionTime.get(pid));
            }
        } finally {
            if (txnId > 0) {
                transactionMgr.abortTransaction(db.getId(), txnId, "finish historical consume offset test");
            }
        }
    }

    @Test
    public void testHistoricalConsumeConcurrentCommitPrevMissing() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> partitionIds = new ArrayList<>(baseTable.getPartitionIds());
        Map<Long, Long> prev = Maps.newHashMap();
        Map<Long, Long> next = Maps.newHashMap();
        for (Long partitionId : partitionIds) {
            Pair<Long, Long> streamUpdate = stream.getStreamUpdate(partitionId);
            if (streamUpdate.first != null) {
                prev.put(partitionId, streamUpdate.first);
            }
            next.put(partitionId, streamUpdate.second);
        }
        OlapTableStreamUpdate updateAtReadTime = new OlapTableStreamUpdate(prev, next);

        long committedTime = System.currentTimeMillis();
        stream.writeLock();
        try {
            stream.unprotectedUpdateStreamUpdate(updateAtReadTime, committedTime);
        } finally {
            stream.writeUnlock();
        }

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
        long txnId = -1L;
        try {
            txnId = transactionMgr.beginTransaction(db.getId(), Collections.singletonList(targetTable.getId()),
                    "ut_table_stream_hist_conflict_" + System.nanoTime(), coordinator,
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

            TransactionState transactionState = transactionMgr.getTransactionState(db.getId(), txnId);
            transactionState.setStreamUpdateInfos(Collections.singletonList(
                    new TableStreamUpdateInfo(db.getId(), stream.getId(), updateAtReadTime)));

            DatabaseTransactionMgr dbTxnMgr = transactionMgr.getDatabaseTransactionMgr(db.getId());
            TransactionCommitFailedException exception;
            stream.writeLock();
            try {
                exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                        () -> Deencapsulation.invoke(dbTxnMgr, "checkStreamOffset", transactionState));
            } finally {
                stream.writeUnlock();
            }
            Assertions.assertTrue(exception.getMessage().contains("previous version missing"));
        } finally {
            if (txnId > 0) {
                transactionMgr.abortTransaction(db.getId(), txnId, "finish historical consume conflict test");
            }
        }
    }

    @Test
    public void testAlterStreamOffsetWatermarkWaitsForPreviousStreamTxn() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        DatabaseTransactionMgr dbTxnMgr = transactionMgr.getDatabaseTransactionMgr(db.getId());
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
        long txnId = transactionMgr.beginTransaction(db.getId(), Collections.singletonList(targetTable.getId()),
                "ut_table_stream_watermark_prev_" + System.nanoTime(), coordinator,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        GlobalTransactionMgr.AlterStreamOffsetContext context = null;
        try {
            TransactionState transactionState = transactionMgr.getTransactionState(db.getId(), txnId);
            transactionState.setStreamUpdateInfos(Collections.singletonList(
                    new TableStreamUpdateInfo(db.getId(), stream.getId(),
                            buildCurrentOlapStreamUpdate(baseTable, stream))));
            dbTxnMgr.registerTableStreamTxn(txnId, transactionState.getStreamUpdateInfos());

            context = transactionMgr.beginAlterStreamOffset(db.getId(), stream.getId());
            Assertions.assertTrue(txnId <= context.getWatermarkTxnId());
            Assertions.assertFalse(transactionMgr.isAlterStreamOffsetReady(context));

            transactionMgr.abortTransaction(db.getId(), txnId, "finish watermark test");
            Assertions.assertTrue(transactionMgr.isAlterStreamOffsetReady(context));
        } finally {
            if (context != null) {
                transactionMgr.endAlterStreamOffset(context);
            }
        }
    }

    @Test
    public void testAlterStreamOffsetWatermarkRejectsFutureStreamTxn() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) db.getTableOrMetaException("tbl_target");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        DatabaseTransactionMgr dbTxnMgr = transactionMgr.getDatabaseTransactionMgr(db.getId());
        GlobalTransactionMgr.AlterStreamOffsetContext context = transactionMgr.beginAlterStreamOffset(
                db.getId(), stream.getId());
        long txnId = -1L;
        try {
            TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                    TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
            txnId = transactionMgr.beginTransaction(db.getId(), Collections.singletonList(targetTable.getId()),
                    "ut_table_stream_watermark_future_" + System.nanoTime(), coordinator,
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            Assertions.assertTrue(txnId > context.getWatermarkTxnId());

            TransactionState transactionState = transactionMgr.getTransactionState(db.getId(), txnId);
            transactionState.setStreamUpdateInfos(Collections.singletonList(
                    new TableStreamUpdateInfo(db.getId(), stream.getId(),
                            buildCurrentOlapStreamUpdate(baseTable, stream))));
            long registeredTxnId = txnId;
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> dbTxnMgr.registerTableStreamTxn(registeredTxnId,
                            transactionState.getStreamUpdateInfos()));
            Assertions.assertTrue(exception.getMessage().contains("ALTER STREAM SET offset is pending"));
        } finally {
            if (txnId > 0) {
                transactionMgr.abortTransaction(db.getId(), txnId, "finish watermark test");
            }
            transactionMgr.endAlterStreamOffset(context);
        }
    }

    @Test
    public void testAlterStreamOffsetRejectsLaterAlterUntilPreviousEnds() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        GlobalTransactionMgr.AlterStreamOffsetContext firstContext = transactionMgr.beginAlterStreamOffset(
                db.getId(), stream.getId());
        try {
            UserException pendingException = Assertions.assertThrows(UserException.class,
                    () -> transactionMgr.beginAlterStreamOffset(db.getId(), stream.getId()));
            Assertions.assertTrue(pendingException.getMessage().contains("current ALTER STREAM SET offset is "
                    + "cancelled"));
            Assertions.assertTrue(transactionMgr.isAlterStreamOffsetReady(firstContext));

            transactionMgr.cancelAlterStreamOffset(db.getId(), stream.getId());
            UserException cancelledException = Assertions.assertThrows(UserException.class,
                    () -> transactionMgr.beginAlterStreamOffset(db.getId(), stream.getId()));
            Assertions.assertTrue(cancelledException.getMessage().contains("current ALTER STREAM SET offset is "
                    + "cancelled"));
        } finally {
            transactionMgr.endAlterStreamOffset(firstContext);
        }

        GlobalTransactionMgr.AlterStreamOffsetContext secondContext = null;
        try {
            secondContext = transactionMgr.beginAlterStreamOffset(db.getId(), stream.getId());
            Assertions.assertTrue(transactionMgr.isAlterStreamOffsetReady(secondContext));
        } finally {
            if (secondContext != null) {
                transactionMgr.endAlterStreamOffset(secondContext);
            }
        }
    }

    @Test
    public void testAlterStreamOffsetWaitsForCrossDbPreviousStreamTxn() throws Exception {
        Database streamDb = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        Database targetDb = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_target");
        OlapTable baseTable = (OlapTable) streamDb.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) targetDb.getTableOrMetaException("tbl_target_cross_db");
        OlapTableStream stream = (OlapTableStream) streamDb.getTableOrMetaException("s1");

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        DatabaseTransactionMgr targetDbTxnMgr = transactionMgr.getDatabaseTransactionMgr(targetDb.getId());
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
        long txnId = transactionMgr.beginTransaction(targetDb.getId(), Collections.singletonList(targetTable.getId()),
                "ut_table_stream_cross_db_prev_" + System.nanoTime(), coordinator,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        GlobalTransactionMgr.AlterStreamOffsetContext context = null;
        try {
            TransactionState transactionState = transactionMgr.getTransactionState(targetDb.getId(), txnId);
            transactionState.setStreamUpdateInfos(Collections.singletonList(
                    new TableStreamUpdateInfo(streamDb.getId(), stream.getId(),
                            buildCurrentOlapStreamUpdate(baseTable, stream))));
            targetDbTxnMgr.registerTableStreamTxn(txnId, transactionState.getStreamUpdateInfos());

            context = transactionMgr.beginAlterStreamOffset(streamDb.getId(), stream.getId());
            Assertions.assertTrue(txnId <= context.getWatermarkTxnId());
            Assertions.assertFalse(transactionMgr.isAlterStreamOffsetReady(context));

            transactionMgr.abortTransaction(targetDb.getId(), txnId, "finish cross db watermark test");
            txnId = -1L;
            Assertions.assertTrue(transactionMgr.isAlterStreamOffsetReady(context));
        } finally {
            if (txnId > 0) {
                transactionMgr.abortTransaction(targetDb.getId(), txnId, "finish cross db watermark test");
            }
            if (context != null) {
                transactionMgr.endAlterStreamOffset(context);
            }
        }
    }

    @Test
    public void testAlterStreamOffsetRejectsCrossDbFutureStreamTxn() throws Exception {
        Database streamDb = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        Database targetDb = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_target");
        OlapTable baseTable = (OlapTable) streamDb.getTableOrMetaException("tbl_stream_base");
        OlapTable targetTable = (OlapTable) targetDb.getTableOrMetaException("tbl_target_cross_db");
        OlapTableStream stream = (OlapTableStream) streamDb.getTableOrMetaException("s1");

        GlobalTransactionMgr transactionMgr = (GlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr();
        DatabaseTransactionMgr targetDbTxnMgr = transactionMgr.getDatabaseTransactionMgr(targetDb.getId());
        GlobalTransactionMgr.AlterStreamOffsetContext context = transactionMgr.beginAlterStreamOffset(
                streamDb.getId(), stream.getId());
        long txnId = -1L;
        try {
            TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                    TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
            txnId = transactionMgr.beginTransaction(targetDb.getId(), Collections.singletonList(targetTable.getId()),
                    "ut_table_stream_cross_db_future_" + System.nanoTime(), coordinator,
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            Assertions.assertTrue(txnId > context.getWatermarkTxnId());

            TransactionState transactionState = transactionMgr.getTransactionState(targetDb.getId(), txnId);
            transactionState.setStreamUpdateInfos(Collections.singletonList(
                    new TableStreamUpdateInfo(streamDb.getId(), stream.getId(),
                            buildCurrentOlapStreamUpdate(baseTable, stream))));
            long registeredTxnId = txnId;
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> targetDbTxnMgr.registerTableStreamTxn(registeredTxnId,
                            transactionState.getStreamUpdateInfos()));
            Assertions.assertTrue(exception.getMessage().contains("ALTER STREAM SET offset is pending"));
        } finally {
            if (txnId > 0) {
                transactionMgr.abortTransaction(targetDb.getId(), txnId, "finish cross db watermark test");
            }
            transactionMgr.endAlterStreamOffset(context);
        }
    }

    private OlapTableStreamUpdate buildCurrentOlapStreamUpdate(OlapTable baseTable, OlapTableStream stream) {
        Map<Long, Long> prev = Maps.newHashMap();
        Map<Long, Long> next = Maps.newHashMap();
        for (Long partitionId : baseTable.getPartitionIds()) {
            Pair<Long, Long> streamUpdate = stream.getStreamUpdate(partitionId);
            if (streamUpdate.first != null) {
                prev.put(partitionId, streamUpdate.first);
            }
            next.put(partitionId, streamUpdate.second);
        }
        return new OlapTableStreamUpdate(prev, next);
    }
}

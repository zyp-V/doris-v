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

import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerDe() throws IOException {
        setMetaVersion(FeMetaVersion.VERSION_CURRENT);

        TransactionState transactionState = newTransactionState();
        Map<String, String> txnExtraInfo = Maps.newHashMap();
        txnExtraInfo.put("k1", "v1");
        txnExtraInfo.put("k2", "v2");
        transactionState.setTxnExtraInfo(txnExtraInfo);

        writeTransactionState(transactionState);
        TransactionState readTransactionState = readTransactionState();

        Assert.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
        Assert.assertEquals(txnExtraInfo, readTransactionState.getTxnExtraInfo());
    }

    @Test
    public void testTxnExtraInfoDefaultEmpty() {
        TransactionState transactionState = new TransactionState();
        Assert.assertNotNull(transactionState.getTxnExtraInfo());
        Assert.assertTrue(transactionState.getTxnExtraInfo().isEmpty());
    }

    @Test
    public void testSerDeWithOldMetaVersion() throws IOException {
        setMetaVersion(FeMetaVersion.VERSION_129);

        TransactionState transactionState = newTransactionState();
        Map<String, String> txnExtraInfo = Maps.newHashMap();
        txnExtraInfo.put("k1", "v1");
        transactionState.setTxnExtraInfo(txnExtraInfo);

        writeTransactionState(transactionState);
        TransactionState readTransactionState = readTransactionState();

        Assert.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
        Assert.assertTrue(readTransactionState.getTxnExtraInfo().isEmpty());
    }

    @Test
    public void testStreamUpdateInfosStoredInTxnExtraInfo() throws IOException {
        setMetaVersion(FeMetaVersion.VERSION_CURRENT);

        TransactionState transactionState = newTransactionState();
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(
                Collections.singletonMap(10L, 100L), Collections.singletonMap(10L, 101L));
        transactionState.setStreamUpdateInfos(Collections.singletonList(
                new TableStreamUpdateInfo(1000L, 2000L, update)));

        Assert.assertTrue(transactionState.getTxnExtraInfo().containsKey("sui"));

        writeTransactionState(transactionState);
        TransactionState readTransactionState = readTransactionState();

        Assert.assertTrue(readTransactionState.getTxnExtraInfo().containsKey("sui"));
        List<TableStreamUpdateInfo> readStreamUpdateInfos = readTransactionState.getStreamUpdateInfos();
        Assert.assertNotNull(readStreamUpdateInfos);
        Assert.assertEquals(1, readStreamUpdateInfos.size());
        TableStreamUpdateInfo readInfo = readStreamUpdateInfos.get(0);
        Assert.assertEquals(1000L, readInfo.getDbId());
        Assert.assertEquals(2000L, readInfo.getStreamId());
        Assert.assertTrue(readInfo.getUpdate() instanceof OlapTableStreamUpdate);
        OlapTableStreamUpdate readUpdate = (OlapTableStreamUpdate) readInfo.getUpdate();
        Assert.assertEquals(update.getPrev(), readUpdate.getPrev());
        Assert.assertEquals(update.getNext(), readUpdate.getNext());
    }

    private void setMetaVersion(int version) {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(version);
        metaContext.setThreadLocalInfo();
    }

    private TransactionState newTransactionState() {
        UUID uuid = UUID.randomUUID();
        return new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", System.currentTimeMillis()),
                50000L, 60 * 1000L);
    }

    private void writeTransactionState(TransactionState transactionState) throws IOException {
        File file = new File(fileName);
        file.createNewFile();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
            transactionState.write(out);
            out.flush();
        }
    }

    private TransactionState readTransactionState() throws IOException {
        File file = new File(fileName);
        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            TransactionState readTransactionState = new TransactionState();
            readTransactionState.readFields(in);
            return readTransactionState;
        }
    }

}

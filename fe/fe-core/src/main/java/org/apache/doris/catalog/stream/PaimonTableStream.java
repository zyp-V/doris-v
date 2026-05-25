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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.stream.PaimonTableStreamWrapper.PaimonStreamMetricInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PaimonTableStream extends BaseTableStream {
    private static final Logger LOG = LogManager.getLogger(PaimonTableStream.class);
    public static final long INVALID_HISTORICAL_SNAPSHOT_ID = -1L;
    private static final String SNAPSHOT_ID_OFFSET_TYPE = "Paimon Snapshot ID";
    private static final String SNAPSHOT_COMMIT_TIME_OFFSET_TYPE = "Paimon Snapshot Commit Time";

    @SerializedName("si")
    private long snapshotId;

    @SerializedName("ctm")
    private long commitTimestampMs;

    @SerializedName("hsi")
    private long historicalSnapshotId = INVALID_HISTORICAL_SNAPSHOT_ID;

    public PaimonTableStream() {
        super();
    }

    public PaimonTableStream(long id, String streamName, List<Column> fullSchema, TableIf baseTable) {
        super(id, streamName, fullSchema, baseTable);
        Preconditions.checkState(baseTable instanceof PaimonExternalTable);
        this.snapshotId = 0L;
        this.commitTimestampMs = 0L;
        this.historicalSnapshotId = INVALID_HISTORICAL_SNAPSHOT_ID;
    }

    public PaimonTableStream(String streamName, List<Column> fullSchema, TableIf baseTable) {
        this(-1, streamName, fullSchema, baseTable);
    }

    @Override
    public String getTableStreamType() {
        return "PAIMON_TABLE_STREAM";
    }

    @Override
    public PaimonExternalTable getBaseTableNullable() {
        TableIf baseTable = super.getBaseTableNullable();
        if (baseTable == null) {
            return null;
        }
        Preconditions.checkState(baseTable instanceof PaimonExternalTable);
        return (PaimonExternalTable) baseTable;
    }

    @Override
    public void setProperties(Map<String, String> properties) throws AnalysisException {
        super.setProperties(properties);
        PaimonSnapshot latestSnapshot = loadLatestSnapshot();
        if (showInitialRows) {
            long latestSnapshotId = latestSnapshot == null ? 0L : latestSnapshot.getSnapshotId();
            this.snapshotId = 0L;
            this.commitTimestampMs = 0L;
            this.historicalSnapshotId = latestSnapshotId > 0
                    ? latestSnapshotId
                    : INVALID_HISTORICAL_SNAPSHOT_ID;
        } else if (latestSnapshot != null) {
            this.snapshotId = latestSnapshot.getSnapshotId();
            this.commitTimestampMs = latestSnapshot.getCommitTimestampMs();
            this.historicalSnapshotId = INVALID_HISTORICAL_SNAPSHOT_ID;
        }
    }

    private PaimonSnapshot loadLatestSnapshot() throws AnalysisException {
        PaimonExternalTable table = getBaseTableNullable();
        if (table == null) {
            return null;
        }
        try {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(
                    table.getCatalog().getId(), table.getDbName(), table.getName());
            PaimonSnapshotCacheValue cacheValue = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getPaimonMetadataCache()
                    .getPaimonSnapshot(table.getCatalog(), table.getDbName(), table.getName());
            if (cacheValue == null || cacheValue.getSnapshot() == null
                    || cacheValue.getSnapshot().getSnapshotId() <= 0) {
                return null;
            }
            return cacheValue.getSnapshot();
        } catch (RuntimeException e) {
            throw new AnalysisException(String.format("Failed to load latest Paimon snapshot for %s.%s.%s: %s",
                    table.getCatalog().getName(), table.getDbName(), table.getName(), e.getMessage()), e);
        }
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getCommitTimestampMs() {
        return commitTimestampMs;
    }

    public long getHistoricalSnapshotId() {
        return historicalSnapshotId;
    }

    public boolean hasHistoricalSnapshot() {
        return historicalSnapshotId > 0;
    }

    public long getCommittedSnapshotId() {
        return snapshotId;
    }

    public long getConsumedSnapshotId() {
        return snapshotId;
    }

    public long getConsumedCommitTimestampMs() {
        return commitTimestampMs;
    }

    public void setSnapshotOffset(long snapshotId, long commitTimestampMs, long historicalSnapshotId) {
        this.snapshotId = snapshotId;
        this.commitTimestampMs = commitTimestampMs;
        this.historicalSnapshotId = historicalSnapshotId;
    }

    public void setCommittedSnapshotOffset(long snapshotId, long commitTimestampMs) {
        setSnapshotOffset(snapshotId, commitTimestampMs, INVALID_HISTORICAL_SNAPSHOT_ID);
    }

    @Override
    void fillTableStreamConsumptionInfo(List<TRow> dataBatch) {
        String baseTableName = getBaseTableNameForConsumption();
        PaimonStreamMetricInfo metricInfo = getPaimonStreamMetricInfo();
        if (metricInfo == null) {
            addConsumptionInfoRow(dataBatch, baseTableName, SNAPSHOT_ID_OFFSET_TYPE, getConsumedSnapshotId(),
                    UNKNOWN_STREAM_CONSUMPTION_VALUE, getConsumedCommitTimestampMs());
            addConsumptionInfoRow(dataBatch, baseTableName, SNAPSHOT_COMMIT_TIME_OFFSET_TYPE,
                    getConsumedCommitTimestampMs(),
                    UNKNOWN_STREAM_CONSUMPTION_VALUE, getConsumedCommitTimestampMs());
            return;
        }
        addConsumptionInfoRow(dataBatch, baseTableName, SNAPSHOT_ID_OFFSET_TYPE, metricInfo.getConsumedSnapshotId(),
                metricInfo.getSnapshotLag(), metricInfo.getConsumedCommitTimestampMs());
        addConsumptionInfoRow(dataBatch, baseTableName, SNAPSHOT_COMMIT_TIME_OFFSET_TYPE,
                metricInfo.getConsumedCommitTimestampMs(),
                metricInfo.getCommitTimeLagMs(), metricInfo.getConsumedCommitTimestampMs());
    }

    private String getBaseTableNameForConsumption() {
        try {
            PaimonExternalTable table = getBaseTableNullable();
            return table == null ? "N/A" : table.getName();
        } catch (RuntimeException e) {
            LOG.debug("Failed to collect Paimon stream base table name for {}", name, e);
            return "N/A";
        }
    }

    private void addConsumptionInfoRow(List<TRow> dataBatch, String baseTableName, String offsetType,
            long consumptionStatus, long lag, long lastConsumptionTime) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(qualifiedDbName));
        trow.addToColumnValue(new TCell().setStringVal(name));
        trow.addToColumnValue(new TCell().setLongVal(id));
        trow.addToColumnValue(new TCell().setStringVal(baseTableName));
        trow.addToColumnValue(new TCell().setStringVal(offsetType));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(consumptionStatus)));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(lag)));
        trow.addToColumnValue(new TCell().setLongVal(lastConsumptionTime));
        dataBatch.add(trow);
    }

    private PaimonStreamMetricInfo getPaimonStreamMetricInfo() {
        try {
            PaimonExternalTable table = getBaseTableNullable();
            if (table == null) {
                return null;
            }
            return new PaimonTableStreamWrapper(this, table).getMetricInfo(true);
        } catch (RuntimeException e) {
            LOG.debug("Failed to collect Paimon stream consumption info for {}", name, e);
            return null;
        }
    }

    @Override
    public void unprotectedCheckStreamUpdate(AbstractTableStreamUpdate update) throws UserException {
        Preconditions.checkArgument(update instanceof PaimonTableStreamUpdate);
        ((PaimonTableStreamUpdate) update).checkSnapshotOffset(getDBName(), getName(), snapshotId,
                historicalSnapshotId);
    }

    @Override
    public void unprotectedUpdateStreamUpdate(AbstractTableStreamUpdate update, Long ts) {
        PaimonTableStreamUpdate paimonUpdate = (PaimonTableStreamUpdate) update;
        this.snapshotId = paimonUpdate.getNextSnapshotId();
        this.commitTimestampMs = paimonUpdate.getNextCommitTimestampMs();
        this.historicalSnapshotId = INVALID_HISTORICAL_SNAPSHOT_ID;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        Text.writeString(out, PaimonTableStream.class.getCanonicalName());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LOG.warn("read fields not supported in stream");
    }

    public static BaseTableStream read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PaimonTableStream.class);
    }
}

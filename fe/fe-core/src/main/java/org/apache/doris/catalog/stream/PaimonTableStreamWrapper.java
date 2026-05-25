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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;
import org.apache.doris.datasource.paimon.PaimonSnapshotOutOfRangeException;
import org.apache.doris.datasource.paimon.PaimonUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// Runtime-only table wrapper for querying a Paimon-backed Doris stream.
public class PaimonTableStreamWrapper extends PaimonExternalTable {
    private static final Logger LOG = LogManager.getLogger(PaimonTableStreamWrapper.class);

    public static final String START_SNAPSHOT_ID_PARAM = "startSnapshotId";
    public static final String END_SNAPSHOT_ID_PARAM = "endSnapshotId";
    public static final String FULL_SNAPSHOT_ID_PARAM = "snapshotId";
    public static final String END_COMMIT_TIMESTAMP_MS_PARAM = "endCommitTimestampMs";
    public static final String INCREMENTAL_BETWEEN_SCAN_MODE_PARAM = "incrementalBetweenScanMode";
    public static final String EMPTY_READ_PARAM = "emptyRead";
    public static final String SOURCE_STREAM_SNAPSHOT_ID_PARAM = "sourceStreamSnapshotId";
    public static final String SOURCE_STREAM_COMMIT_TIMESTAMP_MS_PARAM = "sourceStreamCommitTimestampMs";
    public static final String SOURCE_STREAM_HISTORICAL_SNAPSHOT_ID_PARAM = "sourceStreamHistoricalSnapshotId";
    public static final long UNKNOWN_METRIC_VALUE = -1L;

    private final PaimonTableStream stream;
    private final PaimonExternalTable baseTable;
    private PaimonTableStreamUpdate plannedUpdate;

    public PaimonTableStreamWrapper(PaimonTableStream stream, PaimonExternalTable baseTable) {
        super(baseTable.getId(), baseTable.getName(), baseTable.getRemoteName(),
                (PaimonExternalCatalog) baseTable.getCatalog(), (PaimonExternalDatabase) baseTable.getDatabase());
        this.stream = stream;
        this.baseTable = baseTable;
    }

    public TableScanParams synthesizeScanParams() throws UserException {
        return synthesizeScanParams(null);
    }

    public TableScanParams synthesizeScanParams(TableScanParams userScanParams) throws UserException {
        validateUserScanParams(userScanParams);
        TableScanParams scanParams;
        if (isFullReadScan(userScanParams)) {
            scanParams = synthesizeFullReadParams(userScanParams);
        } else if (stream.hasHistoricalSnapshot()) {
            scanParams = synthesizeHistoricalFullReadParams();
        } else {
            scanParams = synthesizeIncrementalReadParams();
        }
        return withSourceStreamOffsetParams(scanParams);
    }

    @VisibleForTesting
    static void validateUserScanParams(TableScanParams userScanParams) throws UserException {
        if (userScanParams == null || isFullReadScan(userScanParams)) {
            return;
        }
        throw new UserException("Paimon Stream only supports full read scan params. "
                + "Use paimon_stream_read_mode=full, for full reads; "
                + "incremental stream offsets are managed by the stream itself.");
    }

    public static boolean isFullReadScan(TableScanParams scanParams) {
        return scanParams != null && scanParams.fullRead();
    }

    public static boolean isEmptyReadScan(TableScanParams scanParams) {
        return scanParams != null && Boolean.parseBoolean(scanParams.getParams().get(EMPTY_READ_PARAM));
    }

    private TableScanParams synthesizeIncrementalReadParams() throws UserException {
        long prevSnapshotId = stream.getSnapshotId();
        PaimonSnapshot latestSnapshot = getLatestSnapshot();
        if (isEmptyLatestSnapshot(latestSnapshot)) {
            return synthesizeEmptyIncrementalReadParams(prevSnapshotId);
        }
        long endSnapshotId = latestSnapshot.getSnapshotId();
        long endCommitTimestampMs = latestSnapshot.getCommitTimestampMs();
        long startSnapshotId = normalizeStartSnapshotId(stream.getStreamConsumeType(), prevSnapshotId, endSnapshotId);
        if (endSnapshotId < startSnapshotId) {
            endSnapshotId = startSnapshotId;
        }
        ImmutableMap.Builder<String, String> params = ImmutableMap.<String, String>builder()
                .put(START_SNAPSHOT_ID_PARAM, String.valueOf(startSnapshotId))
                .put(END_SNAPSHOT_ID_PARAM, String.valueOf(endSnapshotId))
                .put(END_COMMIT_TIMESTAMP_MS_PARAM, String.valueOf(endCommitTimestampMs));
        if (stream.getStreamConsumeType() == BaseTableStream.StreamConsumeType.MIN_DELTA) {
            params.put(INCREMENTAL_BETWEEN_SCAN_MODE_PARAM, "diff");
        } else if (stream.getStreamConsumeType() == BaseTableStream.StreamConsumeType.APPEND_ONLY) {
            params.put(INCREMENTAL_BETWEEN_SCAN_MODE_PARAM, "changelog");
        }
        plannedUpdate = new PaimonTableStreamUpdate(prevSnapshotId, endSnapshotId, endCommitTimestampMs);
        LOG.debug("Synthesized Paimon incremental stream params for {}: start={}, end={}",
                stream.getName(), startSnapshotId, endSnapshotId);
        return new TableScanParams(TableScanParams.INCREMENTAL_READ, params.build());
    }

    private TableScanParams synthesizeEmptyIncrementalReadParams(long prevSnapshotId) throws UserException {
        if (prevSnapshotId > 0) {
            throw emptySnapshotOutOfRange(prevSnapshotId);
        }
        plannedUpdate = new PaimonTableStreamUpdate(prevSnapshotId, prevSnapshotId, stream.getCommitTimestampMs());
        LOG.debug("Synthesized empty Paimon incremental stream params for {}", stream.getName());
        return buildEmptyReadScanParams(TableScanParams.INCREMENTAL_READ, stream.getCommitTimestampMs());
    }

    @VisibleForTesting
    public static TableScanParams buildEmptyIncrementalReadScanParams(long commitTimestampMs) {
        return buildEmptyReadScanParams(TableScanParams.INCREMENTAL_READ, commitTimestampMs);
    }

    private static TableScanParams buildEmptyReadScanParams(String paramType, long commitTimestampMs) {
        ImmutableMap<String, String> params = ImmutableMap.<String, String>builder()
                .put(EMPTY_READ_PARAM, "true")
                .put(END_COMMIT_TIMESTAMP_MS_PARAM, String.valueOf(commitTimestampMs))
                .build();
        return new TableScanParams(paramType, params);
    }

    private TableScanParams synthesizeFullReadParams(TableScanParams userScanParams) throws UserException {
        long prevSnapshotId = stream.getSnapshotId();
        String requestedSnapshotId = userScanParams.getParams().get(FULL_SNAPSHOT_ID_PARAM);
        if (requestedSnapshotId != null) {
            PaimonSnapshot requestedSnapshot = resolveAlterOffset(requestedSnapshotId,
                    PaimonStreamOffsetType.SNAPSHOT_ID);
            plannedUpdate = new PaimonTableStreamUpdate(prevSnapshotId, requestedSnapshot.getSnapshotId(),
                    requestedSnapshot.getCommitTimestampMs());
            LOG.debug("Synthesized Paimon full stream params for {}: requested snapshot={}", stream.getName(),
                    requestedSnapshot.getSnapshotId());
            return buildFullReadScanParams(requestedSnapshot.getSnapshotId(), requestedSnapshot.getCommitTimestampMs());
        }
        PaimonSnapshot latestSnapshot = getLatestSnapshot();
        if (isEmptyLatestSnapshot(latestSnapshot)) {
            return synthesizeEmptyFullReadParams(prevSnapshotId);
        }
        long snapshotId = latestSnapshot.getSnapshotId();
        long commitTimestampMs = latestSnapshot.getCommitTimestampMs();
        plannedUpdate = new PaimonTableStreamUpdate(prevSnapshotId, snapshotId, commitTimestampMs);
        LOG.debug("Synthesized Paimon full stream params for {}: snapshot={}", stream.getName(), snapshotId);
        return buildFullReadScanParams(snapshotId, commitTimestampMs);
    }

    private TableScanParams synthesizeEmptyFullReadParams(long prevSnapshotId) throws UserException {
        if (prevSnapshotId > 0) {
            throw emptySnapshotOutOfRange(prevSnapshotId);
        }
        plannedUpdate = new PaimonTableStreamUpdate(prevSnapshotId, prevSnapshotId, stream.getCommitTimestampMs());
        LOG.debug("Synthesized empty Paimon full stream params for {}", stream.getName());
        return buildEmptyReadScanParams(TableScanParams.FULL_READ, stream.getCommitTimestampMs());
    }

    private PaimonSnapshotOutOfRangeException emptySnapshotOutOfRange(long prevSnapshotId) {
        return new PaimonSnapshotOutOfRangeException(String.format(
                "Paimon snapshot is out of available range. Stream %s consumed snapshotId %d, "
                        + "but base table %s.%s.%s has no available snapshot.",
                stream.getName(), prevSnapshotId, baseTable.getCatalog().getName(), baseTable.getDbName(),
                baseTable.getName()));
    }

    private TableScanParams synthesizeHistoricalFullReadParams() throws UserException {
        long historicalSnapshotId = stream.getHistoricalSnapshotId();
        // Resolve the commit time lazily so metrics can keep the real consumed offset at 0 before the first read.
        PaimonSnapshot historicalSnapshot = resolveAlterOffset(String.valueOf(historicalSnapshotId),
                PaimonStreamOffsetType.SNAPSHOT_ID);
        plannedUpdate = new PaimonTableStreamUpdate(stream.getSnapshotId(), historicalSnapshotId,
                historicalSnapshot.getCommitTimestampMs());
        LOG.debug("Synthesized Paimon historical full stream params for {}: snapshot={}",
                stream.getName(), historicalSnapshotId);
        return buildHistoricalFullReadScanParams(historicalSnapshotId, historicalSnapshot.getCommitTimestampMs());
    }

    @VisibleForTesting
    public static TableScanParams buildFullReadScanParams(long snapshotId) {
        return buildFullReadScanParams(snapshotId, 0L);
    }

    public static TableScanParams buildFullReadScanParams(long snapshotId, long commitTimestampMs) {
        ImmutableMap<String, String> params = ImmutableMap.<String, String>builder()
                .put(FULL_SNAPSHOT_ID_PARAM, String.valueOf(snapshotId))
                .put(END_SNAPSHOT_ID_PARAM, String.valueOf(snapshotId))
                .put(END_COMMIT_TIMESTAMP_MS_PARAM, String.valueOf(commitTimestampMs))
                .build();
        return new TableScanParams(TableScanParams.FULL_READ, params);
    }

    @VisibleForTesting
    static TableScanParams buildHistoricalFullReadScanParams(long historicalSnapshotId) {
        return buildHistoricalFullReadScanParams(historicalSnapshotId, 0L);
    }

    private static TableScanParams buildHistoricalFullReadScanParams(long historicalSnapshotId,
            long commitTimestampMs) {
        ImmutableMap<String, String> params = ImmutableMap.<String, String>builder()
                .put(FULL_SNAPSHOT_ID_PARAM, String.valueOf(historicalSnapshotId))
                .put(END_SNAPSHOT_ID_PARAM, String.valueOf(historicalSnapshotId))
                .put(END_COMMIT_TIMESTAMP_MS_PARAM, String.valueOf(commitTimestampMs))
                .build();
        return new TableScanParams(TableScanParams.FULL_READ, params);
    }

    private TableScanParams withSourceStreamOffsetParams(TableScanParams scanParams) {
        Map<String, String> params = new HashMap<>(scanParams.getParams());
        params.put(SOURCE_STREAM_SNAPSHOT_ID_PARAM, String.valueOf(stream.getSnapshotId()));
        params.put(SOURCE_STREAM_COMMIT_TIMESTAMP_MS_PARAM, String.valueOf(stream.getCommitTimestampMs()));
        params.put(SOURCE_STREAM_HISTORICAL_SNAPSHOT_ID_PARAM, String.valueOf(stream.getHistoricalSnapshotId()));
        return new TableScanParams(scanParams.getParamType(), params);
    }

    public PaimonTableStreamUpdate getStreamUpdate() {
        Preconditions.checkState(plannedUpdate != null,
                "Paimon stream scan params must be synthesized before collecting stream update");
        return plannedUpdate;
    }

    public Long getStreamDbId() {
        return stream.getDatabase().getId();
    }

    public Long getStreamId() {
        return stream.getId();
    }

    public PaimonTableStream getStream() {
        return stream;
    }

    public PaimonExternalTable getBaseTable() {
        return baseTable;
    }

    public PaimonSnapshot resolveAlterOffset(String offsetValue, PaimonStreamOffsetType offsetType)
            throws UserException {
        if (offsetType == PaimonStreamOffsetType.SNAPSHOT_ID) {
            return resolveSnapshotIdOffset(offsetValue);
        }
        return resolveSnapshotOffsetFromSnapshots(loadSnapshotOffsets(), offsetValue, offsetType);
    }

    private PaimonSnapshot resolveSnapshotIdOffset(String offsetValue) throws UserException {
        long snapshotId = parseOffsetLong(offsetValue);
        try {
            Table snapshotsTable = getSnapshotsTable();
            RowType rowType = snapshotsTable.rowType();
            int snapshotIdIndex = findFieldIndex(rowType, "snapshot_id");
            Predicate predicate = new PredicateBuilder(rowType).equal(snapshotIdIndex, snapshotId);
            List<PaimonSnapshot> snapshots = loadSnapshotOffsets(snapshotsTable, predicate);
            for (PaimonSnapshot snapshot : snapshots) {
                if (snapshot.getSnapshotId() == snapshotId) {
                    return snapshot;
                }
            }
            return resolveSnapshotOffsetFromSnapshots(loadSnapshotOffsets(snapshotsTable, null), offsetValue,
                    PaimonStreamOffsetType.SNAPSHOT_ID);
        } catch (IOException | RuntimeException e) {
            throw new UserException("Failed to load Paimon snapshots table for stream " + stream.getName(), e);
        }
    }

    public PaimonStreamMetricInfo getMetricInfo() {
        return getMetricInfo(false);
    }

    public PaimonStreamMetricInfo getMetricInfo(boolean refreshLatestSnapshot) {
        PaimonSnapshot latestSnapshot = null;
        try {
            PaimonSnapshotCacheValue snapshotCacheValue = getLatestSnapshotCacheValue(refreshLatestSnapshot);
            if (snapshotCacheValue != null) {
                latestSnapshot = snapshotCacheValue.getSnapshot();
            }
        } catch (RuntimeException e) {
            LOG.debug("Failed to load latest Paimon snapshot metric for stream {}", stream.getName(), e);
        }
        return new PaimonStreamMetricInfo(stream.getId(), stream.getQualifiedDbName(), stream.getName(),
                stream.getConsumeType(), baseTable.getCatalog().getName(), baseTable.getDbName(), baseTable.getName(),
                stream.getConsumedSnapshotId(), stream.getConsumedCommitTimestampMs(), latestSnapshot);
    }

    @VisibleForTesting
    public static PaimonSnapshot resolveSnapshotOffsetFromSnapshots(
            List<PaimonSnapshot> snapshots, String offsetValue, PaimonStreamOffsetType offsetType)
            throws PaimonSnapshotOutOfRangeException {
        if (snapshots == null || snapshots.isEmpty()) {
            throw new PaimonSnapshotOutOfRangeException(
                    "Paimon snapshot is out of available range. No available snapshot.");
        }
        long input = parseOffsetLong(offsetValue);
        if (offsetType == PaimonStreamOffsetType.SNAPSHOT_ID) {
            for (PaimonSnapshot snapshot : snapshots) {
                if (snapshot.getSnapshotId() == input) {
                    return snapshot;
                }
            }
            throw outOfRangeForSnapshotId(input, snapshots);
        }
        PaimonSnapshot minTimestampSnapshot = snapshots.get(0);
        PaimonSnapshot target = null;
        for (PaimonSnapshot snapshot : snapshots) {
            if (snapshot.getCommitTimestampMs() < minTimestampSnapshot.getCommitTimestampMs()) {
                minTimestampSnapshot = snapshot;
            }
            if (snapshot.getCommitTimestampMs() >= input
                    && (target == null || snapshot.getCommitTimestampMs() < target.getCommitTimestampMs()
                    || (snapshot.getCommitTimestampMs() == target.getCommitTimestampMs()
                    && snapshot.getSnapshotId() < target.getSnapshotId()))) {
                target = snapshot;
            }
        }
        if (input < minTimestampSnapshot.getCommitTimestampMs() || target == null) {
            throw outOfRangeForTimestamp(input, snapshots);
        }
        return target;
    }

    private List<PaimonSnapshot> loadSnapshotOffsets() throws UserException {
        try {
            // Load the snapshots system table directly. Invalidating the base table cache here would disturb
            // unrelated Paimon queries and does not make this catalog read fresher.
            return loadSnapshotOffsets(getSnapshotsTable(), null);
        } catch (IOException | RuntimeException e) {
            throw new UserException("Failed to load Paimon snapshots table for stream " + stream.getName(), e);
        }
    }

    private Table getSnapshotsTable() {
        String snapshotsTableName = baseTable.getName() + Catalog.SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS;
        return ((PaimonExternalCatalog) baseTable.getCatalog())
                .getPaimonTable(baseTable.getDbName(), snapshotsTableName);
    }

    private List<PaimonSnapshot> loadSnapshotOffsets(Table snapshotsTable,
            Predicate predicate) throws IOException {
        RowType rowType = snapshotsTable.rowType();
        int snapshotIdIndex = findFieldIndex(rowType, "snapshot_id");
        int commitTimeIndex = findFieldIndex(rowType, "commit_time");
        int commitTimePrecision = getTimestampPrecision(rowType.getFields().get(commitTimeIndex).type());
        List<InternalRow> rows = PaimonUtil.read(snapshotsTable, new int[] {snapshotIdIndex, commitTimeIndex},
                predicate);
        List<PaimonSnapshot> snapshots = new ArrayList<>(rows.size());
        for (InternalRow row : rows) {
            snapshots.add(new PaimonSnapshot(row.getLong(0), 0L,
                    row.getTimestamp(1, commitTimePrecision).getMillisecond()));
        }
        snapshots.sort((left, right) -> Long.compare(left.getSnapshotId(), right.getSnapshotId()));
        return snapshots;
    }

    private PaimonSnapshot getLatestSnapshot() {
        PaimonSnapshotCacheValue snapshotCacheValue = getLatestSnapshotCacheValue(true);
        return snapshotCacheValue == null ? null : snapshotCacheValue.getSnapshot();
    }

    private PaimonSnapshotCacheValue getLatestSnapshotCacheValue(boolean refreshCache) {
        if (refreshCache) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(
                    baseTable.getCatalog().getId(), baseTable.getDbName(), baseTable.getName());
        }
        return Env.getCurrentEnv().getExtMetaCacheMgr().getPaimonMetadataCache()
                .getPaimonSnapshot(baseTable.getCatalog(), baseTable.getDbName(), baseTable.getName());
    }

    @VisibleForTesting
    static long normalizeStartSnapshotId(BaseTableStream.StreamConsumeType streamType, long startSnapshotId,
            long endSnapshotId) {
        if (streamType == BaseTableStream.StreamConsumeType.MIN_DELTA && startSnapshotId == 0 && endSnapshotId > 0) {
            return 1L;
        }
        return startSnapshotId;
    }

    private static boolean isEmptyLatestSnapshot(PaimonSnapshot latestSnapshot) {
        return latestSnapshot == null || latestSnapshot.getSnapshotId() <= 0;
    }

    private static int findFieldIndex(RowType rowType, String fieldName) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fieldName.equalsIgnoreCase(fields.get(i).name())) {
                return i;
            }
        }
        throw new IllegalStateException("Can not find field " + fieldName + " in Paimon snapshots table");
    }

    private static int getTimestampPrecision(DataType dataType) {
        if (dataType instanceof TimestampType) {
            return ((TimestampType) dataType).getPrecision();
        }
        if (dataType instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) dataType).getPrecision();
        }
        return 3;
    }

    private static long parseOffsetLong(String offsetValue) throws PaimonSnapshotOutOfRangeException {
        try {
            return Long.parseLong(offsetValue);
        } catch (NumberFormatException e) {
            throw new PaimonSnapshotOutOfRangeException("Invalid Paimon stream offset: " + offsetValue, e);
        }
    }

    private static PaimonSnapshotOutOfRangeException outOfRangeForSnapshotId(
            long snapshotId, List<PaimonSnapshot> snapshots) {
        return new PaimonSnapshotOutOfRangeException(String.format(
                "Paimon snapshot is out of available range. Requested snapshotId %d, available snapshotId range %s.",
                snapshotId, snapshotIdRange(snapshots)));
    }

    private static PaimonSnapshotOutOfRangeException outOfRangeForTimestamp(
            long timestampMs, List<PaimonSnapshot> snapshots) {
        return new PaimonSnapshotOutOfRangeException(String.format(
                "Paimon snapshot is out of available range. Requested timestamp %d, "
                        + "available commit timestamp range %s, available snapshotId range %s.",
                timestampMs, commitTimestampRange(snapshots), snapshotIdRange(snapshots)));
    }

    private static String snapshotIdRange(List<PaimonSnapshot> snapshots) {
        long minSnapshotId = Long.MAX_VALUE;
        long maxSnapshotId = Long.MIN_VALUE;
        for (PaimonSnapshot snapshot : snapshots) {
            minSnapshotId = Math.min(minSnapshotId, snapshot.getSnapshotId());
            maxSnapshotId = Math.max(maxSnapshotId, snapshot.getSnapshotId());
        }
        return String.format("[%d, %d]", minSnapshotId, maxSnapshotId);
    }

    private static String commitTimestampRange(List<PaimonSnapshot> snapshots) {
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        for (PaimonSnapshot snapshot : snapshots) {
            minTimestamp = Math.min(minTimestamp, snapshot.getCommitTimestampMs());
            maxTimestamp = Math.max(maxTimestamp, snapshot.getCommitTimestampMs());
        }
        return String.format("[%d, %d]", minTimestamp, maxTimestamp);
    }

    @Override
    public Table getPaimonTable(Optional<MvccSnapshot> snapshot) {
        return baseTable.getPaimonTable(snapshot);
    }

    @Override
    public Table getPaimonRawTable() {
        return baseTable.getPaimonRawTable();
    }

    @Override
    public List<Column> getFullSchema() {
        return appendPaimonRowKindIfNeeded(stream.getFullSchema());
    }

    @Override
    public List<Column> getBaseSchema() {
        return appendPaimonRowKindIfNeeded(stream.getBaseSchema());
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return appendPaimonRowKindIfNeeded(stream.getBaseSchema(full));
    }

    @Override
    public Column getColumn(String name) {
        Column column = stream.getColumn(name);
        if (column == null && shouldExposePaimonRowKind()
                && PaimonExternalTable.PAIMON_ROW_KIND_COLUMN.equalsIgnoreCase(name)) {
            return getExposedPaimonRowKindColumn();
        }
        return column;
    }

    @Override
    public DatabaseIf getDatabase() {
        return baseTable.getDatabase();
    }

    @Override
    public long fetchRowCount() {
        return baseTable.fetchRowCount();
    }

    @Override
    public long getUpdateTime() {
        return baseTable.getUpdateTime();
    }

    private boolean shouldExposePaimonRowKind() {
        return stream.getStreamConsumeType() == BaseTableStream.StreamConsumeType.APPEND_ONLY
                || stream.getStreamConsumeType() == BaseTableStream.StreamConsumeType.MIN_DELTA;
    }

    private List<Column> appendPaimonRowKindIfNeeded(List<Column> schema) {
        if (!shouldExposePaimonRowKind()) {
            return schema;
        }
        boolean exists = schema.stream()
                .anyMatch(column -> column.getName().equalsIgnoreCase(PaimonExternalTable.PAIMON_ROW_KIND_COLUMN));
        if (exists) {
            return schema;
        }
        Column rowKindColumn = getExposedPaimonRowKindColumn();
        if (rowKindColumn == null) {
            return schema;
        }
        List<Column> newSchema = Lists.newArrayListWithCapacity(schema.size() + 1);
        newSchema.addAll(schema);
        newSchema.add(rowKindColumn);
        return newSchema;
    }

    private Column getExposedPaimonRowKindColumn() {
        Column rowKindColumn = baseTable.getColumn(PaimonExternalTable.PAIMON_ROW_KIND_COLUMN);
        if (rowKindColumn == null) {
            return null;
        }
        Column exposedRowKind = new Column(rowKindColumn);
        exposedRowKind.setIsVisible(true);
        return exposedRowKind;
    }

    public static class PaimonStreamMetricInfo {
        private final long streamId;
        private final String dbName;
        private final String streamName;
        private final String streamType;
        private final String baseCatalogName;
        private final String baseDbName;
        private final String baseTableName;
        private final long consumedSnapshotId;
        private final long consumedCommitTimestampMs;
        private final PaimonSnapshot latestSnapshot;

        private PaimonStreamMetricInfo(long streamId, String dbName, String streamName, String streamType,
                String baseCatalogName, String baseDbName, String baseTableName,
                long consumedSnapshotId, long consumedCommitTimestampMs, PaimonSnapshot latestSnapshot) {
            this.streamId = streamId;
            this.dbName = dbName;
            this.streamName = streamName;
            this.streamType = streamType;
            this.baseCatalogName = baseCatalogName;
            this.baseDbName = baseDbName;
            this.baseTableName = baseTableName;
            this.consumedSnapshotId = consumedSnapshotId;
            this.consumedCommitTimestampMs = consumedCommitTimestampMs;
            this.latestSnapshot = latestSnapshot;
        }

        public long getStreamId() {
            return streamId;
        }

        public String getDbName() {
            return dbName;
        }

        public String getStreamName() {
            return streamName;
        }

        public String getStreamType() {
            return streamType;
        }

        public String getBaseCatalogName() {
            return baseCatalogName;
        }

        public String getBaseDbName() {
            return baseDbName;
        }

        public String getBaseTableName() {
            return baseTableName;
        }

        public long getConsumedSnapshotId() {
            return consumedSnapshotId;
        }

        public long getConsumedCommitTimestampMs() {
            return consumedCommitTimestampMs;
        }

        public long getLatestSnapshotId() {
            return latestSnapshot == null ? UNKNOWN_METRIC_VALUE : latestSnapshot.getSnapshotId();
        }

        public long getLatestCommitTimestampMs() {
            return latestSnapshot == null ? UNKNOWN_METRIC_VALUE : latestSnapshot.getCommitTimestampMs();
        }

        public long getSnapshotLag() {
            if (getConsumedSnapshotId() < 0 || getLatestSnapshotId() < 0) {
                return UNKNOWN_METRIC_VALUE;
            }
            return Math.max(0L, getLatestSnapshotId() - getConsumedSnapshotId());
        }

        public long getCommitTimeLagMs() {
            if (getConsumedCommitTimestampMs() <= 0 || getLatestCommitTimestampMs() <= 0) {
                return UNKNOWN_METRIC_VALUE;
            }
            return Math.max(0L, getLatestCommitTimestampMs() - getConsumedCommitTimestampMs());
        }
    }
}

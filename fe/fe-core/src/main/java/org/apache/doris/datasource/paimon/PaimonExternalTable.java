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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonExternalTable extends ExternalTable implements MTMVRelatedTableIf, MTMVBaseTableIf, MvccTable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalTable.class);
    public static final String PAIMON_ROW_KIND_COLUMN = "__paimon_row_kind";

    private Table paimonTable;

    public PaimonExternalTable(long id, String name, String remoteName, PaimonExternalCatalog catalog,
            PaimonExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.PAIMON_EXTERNAL_TABLE);
    }

    public String getPaimonCatalogType() {
        return ((PaimonExternalCatalog) catalog).getCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            this.paimonTable = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName, name);
            objectCreated = true;
        }
    }

    public Table getPaimonTable(Optional<MvccSnapshot> snapshot) {
        makeSureInitialized();
        if (isSystemTableName(name)) {
            return paimonTable;
        }
        return paimonTable.copy(
                Collections.singletonMap(CoreOptions.SCAN_VERSION.key(),
                        String.valueOf(getOrFetchSnapshotCacheValue(snapshot).getSnapshot().getSnapshotId())));
    }

    public Table getPaimonRawTable() {
        makeSureInitialized();
        return paimonTable;
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(long schemaId) {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog);
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(
                new PaimonSchemaCacheKey(dbName, name, schemaId));
        if (!schemaCacheValue.isPresent()) {
            throw new CacheException("failed to getSchema for: %s.%s.%s.%s",
                    null, catalog.getName(), dbName, name, schemaId);
        }
        return (PaimonSchemaCacheValue) schemaCacheValue.get();
    }

    private PaimonSnapshotCacheValue getPaimonSnapshotCacheValue() {
        makeSureInitialized();
        return Env.getCurrentEnv().getExtMetaCacheMgr().getPaimonMetadataCache()
                .getPaimonSnapshot(catalog, dbName, name);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (PaimonExternalCatalog.PAIMON_HMS.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_FILESYSTEM.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_DLF.equals(getPaimonCatalogType())) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms/filesystem catalog,not support :"
                    + getPaimonCatalogType());
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        try {
            long rowCount = 0;
            List<Split> splits = paimonTable.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                rowCount += split.rowCount();
            }
            return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
        } catch (Exception e) {
            LOG.warn("Fail to collect row count for db {} table {}", dbName, name, e);
        }
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return Maps.newHashMap(getNameToPartitionItems(snapshot));
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        if (isPartitionInvalid(snapshot)) {
            return PartitionType.UNPARTITIONED;
        }
        return getPartitionColumns(snapshot).size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException {
        return getPartitionColumns(snapshot).stream()
            .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        if (isPartitionInvalid(snapshot)) {
            return Collections.emptyList();
        }
        return getPaimonSchemaCacheValue(snapshot).getPartitionColumns();
    }


    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
                                               Optional<MvccSnapshot> snapshot) throws AnalysisException {
        PaimonPartition paimonPartition = getOrFetchSnapshotCacheValue(snapshot)
                .getPartitionInfo().getNameToPartition().get(partitionName);
        if (paimonPartition == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVTimestampSnapshot(paimonPartition.getLastUpdateTime());
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        return new MTMVVersionSnapshot(getLatestSnapshotIdFromCache());
    }

    public long getLatestSnapshotIdFromCache() throws AnalysisException {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            throw new AnalysisException("not present");
        }
        return ((PaimonSchemaCacheValue) schemaCacheValue.get()).getSnapshootId();
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        return true;
    }

    private boolean isPartitionInvalid(Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue paimonSnapshotCacheValue = getOrFetchSnapshotCacheValue(snapshot);
        return paimonSnapshotCacheValue.getPartitionInfo().isPartitionInvalid();
    }

    @Override
    public MvccSnapshot loadSnapshot() {
        return new PaimonMvccSnapshot(getPaimonSnapshotCacheValue());
    }

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        return getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo().getNameToPartitionItem();
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        return true;
    }

    @Override
    public List<Column> getFullSchema() {
        return getPaimonSchemaCacheValue(MvccUtil.getSnapshotFromContext(this)).getSchema();
    }

    @Override
    public List<Column> getBaseSchema() {
        return getBaseSchema(false);
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        if (full) {
            return getFullSchema();
        }
        return getFullSchema().stream().filter(Column::isVisible).collect(Collectors.toList());
    }

    @Override
    public int getBaseColumnIdxByName(String colName) {
        int i = 0;
        for (Column col : getBaseSchema(true)) {
            if (col.getName().equalsIgnoreCase(colName)) {
                return i;
            }
            ++i;
        }
        return -1;
    }

    private Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType) {
        int tsScale = 3; // default
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case VARCHAR:
            case BINARY:
            case CHAR:
            case VARBINARY:
                return Type.STRING;
            case DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.TimestampType) {
                    tsScale = ((org.apache.paimon.types.TimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                } else if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case ARRAY:
                org.apache.paimon.types.ArrayType arrayType = (org.apache.paimon.types.ArrayType) dataType;
                Type innerType = paimonPrimitiveTypeToDorisType(arrayType.getElementType());
                return org.apache.doris.catalog.ArrayType.create(innerType, true);
            case MAP:
                org.apache.paimon.types.MapType mapType = (MapType) dataType;
                return new org.apache.doris.catalog.MapType(
                    paimonTypeToDorisType(mapType.getKeyType()), paimonTypeToDorisType(mapType.getValueType()));
            case ROW:
                RowType rowType = (RowType) dataType;
                List<DataField> fields = rowType.getFields();
                return new org.apache.doris.catalog.StructType(fields.stream()
                    .map(field -> new org.apache.doris.catalog.StructField(field.name(),
                        paimonTypeToDorisType(field.type())))
                    .collect(Collectors.toCollection(ArrayList::new)));
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                LOG.warn("Cannot transform unknown type: " + dataType.getTypeRoot());
                return Type.UNSUPPORTED;
        }
    }

    protected Type paimonTypeToDorisType(org.apache.paimon.types.DataType type) {
        return paimonPrimitiveTypeToDorisType(type);
    }

    private long loadLatestSnapshotId() throws IOException {
        String tableName = getBaseTableName(name);
        Table table = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName,
                tableName + Catalog.SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS);
        // snapshotId
        List<InternalRow> rows = PaimonUtil.read(table, new int[]{0}, null);
        long latestSnapshotId = 0L;
        for (InternalRow row : rows) {
            long snapshotId = row.getLong(0);
            if (snapshotId > latestSnapshotId) {
                latestSnapshotId = snapshotId;
            }
        }
        return latestSnapshotId;
    }

    private PaimonPartitionInfo loadPartitionInfo(List<Column> partitionColumns) throws IOException, AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return new PaimonPartitionInfo();
        }
        List<PaimonPartition> paimonPartitions = loadPartitions();
        return PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions);
    }

    private List<PaimonPartition> loadPartitions()
            throws IOException {
        Table table = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName,
                name + Catalog.SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        List<InternalRow> rows = PaimonUtil.read(table, null, null);
        List<PaimonPartition> res = Lists.newArrayListWithCapacity(rows.size());
        for (InternalRow row : rows) {
            res.add(PaimonUtil.rowToPartition(row));
        }
        return res;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        Table paimonTable = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName, name);
        List<DataField> columns = paimonTable.rowType().getFields();
        return buildSchemaCacheValue(paimonTable, columns, paimonTable.partitionKeys());
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(ExternalSchemaCache.SchemaCacheKey key) {
        if (!(key instanceof PaimonSchemaCacheKey)) {
            return super.initSchema(key);
        }
        try {
            Table paimonTable = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName, name);
            if (isSystemTableName(name)) {
                return buildSchemaCacheValue(
                        paimonTable, paimonTable.rowType().getFields(), paimonTable.partitionKeys());
            }
            PaimonSchema paimonSchema = loadPaimonSchemaBySchemaId((PaimonSchemaCacheKey) key);
            return buildSchemaCacheValue(paimonTable, paimonSchema.getFields(), paimonSchema.getPartitionKeys());
        } catch (IOException e) {
            LOG.warn("failed to init paimon schema by schema id for {}.{}.{}",
                    catalog.getName(), dbName, name, e);
            return Optional.empty();
        }
    }

    private Optional<SchemaCacheValue> buildSchemaCacheValue(Table paimonTable, List<DataField> columns,
            List<String> partitionKeys) {
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
        Set<String> partitionColumnNames = Sets.newHashSet(partitionKeys);
        List<Column> partitionColumns = Lists.newArrayList();
        for (DataField field : columns) {
            Column column = new Column(field.name().toLowerCase(),
                    paimonTypeToDorisType(field.type()),
                    true, null, true, field.description(), true,
                    field.id());
            tmpSchema.add(column);
            if (partitionColumnNames.contains(field.name())) {
                partitionColumns.add(column);
            }
        }
        tmpSchema.add(new Column(PAIMON_ROW_KIND_COLUMN, Type.STRING, true,
                null, true, "Paimon row kind in changelog mode", false, -1));
        try {
            long latestSnapshotId = isSystemTableName(name)
                    ? loadLatestSnapshotId()
                    : paimonTable.latestSnapshot().map(Snapshot::id).orElse(0L);
            PaimonPartitionInfo partitionInfo = new PaimonPartitionInfo();
            return Optional.of(new PaimonSchemaCacheValue(tmpSchema, partitionColumns, paimonTable, latestSnapshotId,
                partitionInfo));
        } catch (Exception e) {
            LOG.warn(e);
            return Optional.empty();
        }
    }

    private PaimonSchema loadPaimonSchemaBySchemaId(PaimonSchemaCacheKey key) throws IOException {
        Table table = ((PaimonExternalCatalog) getCatalog()).getPaimonTable(key.getDbName(),
                getBaseTableName(name) + Catalog.SYSTEM_TABLE_SPLITTER + SchemasTable.SCHEMAS);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate predicate = builder.equal(0, key.getSchemaId());
        // Adding predicates will also return excess data
        List<InternalRow> rows = PaimonUtil.read(table, new int[] {0, 1, 2}, predicate);
        for (InternalRow row : rows) {
            PaimonSchema schema = PaimonUtil.rowToSchema(row);
            if (schema.getSchemaId() == key.getSchemaId()) {
                return schema;
            }
        }
        throw new CacheException("failed to initSchema for: %s.%s.%s.%s",
                null, getCatalog().getName(), key.getDbName(), key.getTblName(), key.getSchemaId());
    }

    private PaimonSchemaCacheValue getPaimonSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue snapshotCacheValue = getOrFetchSnapshotCacheValue(snapshot);
        return getPaimonSchemaCacheValue(snapshotCacheValue.getSnapshot().getSchemaId());
    }

    private PaimonSnapshotCacheValue getOrFetchSnapshotCacheValue(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        } else {
            return getPaimonSnapshotCacheValue();
        }
    }

    private String getBaseTableName(String tableName) {
        int index = tableName.indexOf(Catalog.SYSTEM_TABLE_SPLITTER);
        return index > 0 ? tableName.substring(0, index) : tableName;
    }

    private boolean isSystemTableName(String tableName) {
        return tableName.contains(Catalog.SYSTEM_TABLE_SPLITTER);
    }

    @Override
    public void beforeMTMVRefresh(MTMV mtmv) throws DdlException {
        Env.getCurrentEnv().getRefreshManager()
            .refreshTable(getCatalog().getName(), getDbName(), getName(), true);
    }
}

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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;
import org.apache.doris.paimon.PaimonTableCache.PaimonTableCacheKey;
import org.apache.doris.paimon.PaimonTableCache.TableExt;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniScanner.class);
    private static final String PAIMON_ROW_KIND_COLUMN = "__paimon_row_kind";
    private static final String STREAM_CHANGE_TYPE_COL = "__DORIS_STREAM_CHANGE_TYPE_COL__";
    private static final String STREAM_SEQ_COL = "__DORIS_STREAM_SEQUENCE_COL__";
    private static final String APPEND = "APPEND";
    private static final String DELETE = "DELETE";
    private static final String UPDATE_BEFORE = "UPDATE_BEFORE";
    private static final String UPDATE_AFTER = "UPDATE_AFTER";
    private static final String INCREMENTAL_BETWEEN_SCAN_MODE = "incrementalBetweenScanMode";
    private static final String PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE = "incremental-between-scan-mode";
    private static final String DORIS_PAIMON_STREAM_READ = "doris.paimon.stream.read";
    private static final String DORIS_PAIMON_STREAM_CONSUME_TYPE = "doris.paimon.stream.consume.type";
    private static final String APPEND_ONLY = "APPEND_ONLY";
    private static final String MIN_DELTA = "MIN_DELTA";
    private static final String INSERT_ROW_KIND = "+I";
    private static final String DIFF = "diff";
    @Deprecated
    private static final String PAIMON_OPTION_PREFIX = "paimon.";
    @Deprecated
    private static final String HADOOP_OPTION_PREFIX = "hadoop.";

    private final Map<String, String> params;
    @Deprecated
    private final Map<String, String> paimonOptionParams;
    @Deprecated
    private final Map<String, String> hadoopOptionParams;
    @Deprecated
    private final String dbName;
    @Deprecated
    private final String tblName;
    private final String paimonSplit;
    private final String paimonPredicate;
    private Table table;
    private RecordReader<InternalRow> reader;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();
    private List<String> paimonAllFieldNames;
    private List<DataType> paimonDataTypeList;

    @Deprecated
    private long ctlId;
    @Deprecated
    private long dbId;
    @Deprecated
    private long tblId;
    @Deprecated
    private long lastUpdateTime;
    private RecordReader.RecordIterator<InternalRow> recordIterator = null;
    private final ClassLoader classLoader;
    private PreExecutionAuthenticator preExecutionAuthenticator;
    private int[] fieldToPaimonIndex;
    private int[] fieldToDataTypeIndex;

    public PaimonJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("params:{}", params);
        }
        this.params = params;
        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        paimonSplit = params.get("paimon_split");
        paimonPredicate = params.get("paimon_predicate");
        dbName = params.get("db_name");
        tblName = params.get("table_name");
        ctlId = Long.parseLong(params.get("ctl_id"));
        dbId = Long.parseLong(params.get("db_id"));
        tblId = Long.parseLong(params.get("tbl_id"));
        lastUpdateTime = Long.parseLong(params.get("last_update_time"));
        initTableInfo(columnTypes, requiredFields, batchSize);
        paimonOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(PAIMON_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(PAIMON_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
    }

    @Override
    public void open() throws IOException {
        try {
            // When the user does not specify hive-site.xml, Paimon will look for the file from the classpath:
            //    org.apache.paimon.hive.HiveCatalog.createHiveConf:
            //        `Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE)`
            // so we need to provide a classloader, otherwise it will cause NPE.
            Thread.currentThread().setContextClassLoader(classLoader);
            preExecutionAuthenticator.execute(() -> {
                initTable();
                initReader();
                return null;
            });
            resetDatetimeV2Precision();

        } catch (Throwable e) {
            LOG.warn("Failed to open paimon_scanner: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void initReader() throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        int[] projected = getProjected();
        if (projected.length > 0) {
            readBuilder.withProjection(projected);
        }
        readBuilder.withFilter(getPredicates());
        reader = readBuilder.newRead().executeFilter().createReader(getSplit());
        paimonDataTypeList =
                Arrays.stream(projected).mapToObj(i -> table.rowType().getTypeAt(i)).collect(Collectors.toList());
    }

    private int[] getProjected() {
        fieldToPaimonIndex = new int[fields.length];
        fieldToDataTypeIndex = new int[fields.length];
        Arrays.fill(fieldToPaimonIndex, -1);
        Arrays.fill(fieldToDataTypeIndex, -1);
        List<Integer> projected = new java.util.ArrayList<>();
        int dataTypeIndex = 0;
        for (int i = 0; i < fields.length; i++) {
            if (PAIMON_ROW_KIND_COLUMN.equalsIgnoreCase(fields[i])
                    || STREAM_CHANGE_TYPE_COL.equalsIgnoreCase(fields[i])
                    || STREAM_SEQ_COL.equalsIgnoreCase(fields[i])) {
                continue;
            }
            int paimonIndex = paimonAllFieldNames.indexOf(fields[i].toLowerCase());
            fieldToPaimonIndex[i] = paimonIndex;
            if (paimonIndex >= 0) {
                projected.add(paimonIndex);
                fieldToDataTypeIndex[i] = dataTypeIndex++;
            }
        }
        return projected.stream().mapToInt(Integer::intValue).toArray();
    }

    private List<Predicate> getPredicates() {
        List<Predicate> predicates = PaimonUtils.deserialize(paimonPredicate);
        if (LOG.isDebugEnabled()) {
            LOG.debug("predicates:{}", predicates);
        }
        return predicates;
    }

    private Split getSplit() {
        Split split = PaimonUtils.deserialize(paimonSplit);
        if (LOG.isDebugEnabled()) {
            LOG.debug("split:{}", split);
        }
        return split;
    }

    private void resetDatetimeV2Precision() {
        for (int i = 0; i < types.length; i++) {
            if (types[i].isDateTimeV2()) {
                // paimon support precision > 6, but it has been reset as 6 in FE
                // try to get the right precision for datetimev2
                int index = paimonAllFieldNames.indexOf(fields[i]);
                if (index != -1) {
                    DataType dataType = table.rowType().getTypeAt(index);
                    if (dataType instanceof TimestampType) {
                        types[i].setPrecision(((TimestampType) dataType).getPrecision());
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private int readAndProcessNextBatch() throws IOException {
        int rows = 0;
        try {
            if (recordIterator == null) {
                recordIterator = reader.readBatch();
            }

            while (recordIterator != null) {
                InternalRow record;
                while ((record = recordIterator.next()) != null) {
                    String rowKind = record.getRowKind().shortString();
                    // APPEND_ONLY uses Paimon changelog mode but exposes only inserted rows to Doris stream users.
                    if (shouldSkipRecord(rowKind)) {
                        continue;
                    }
                    columnValue.setOffsetRow(record);
                    for (int i = 0; i < fields.length; i++) {
                        if (PAIMON_ROW_KIND_COLUMN.equalsIgnoreCase(fields[i])) {
                            columnValue.setString(getPaimonRowKindValue(rowKind));
                            appendData(i, columnValue);
                            continue;
                        }
                        if (STREAM_CHANGE_TYPE_COL.equalsIgnoreCase(fields[i])) {
                            columnValue.setString(getChangeType(rowKind));
                            appendData(i, columnValue);
                            continue;
                        }
                        if (STREAM_SEQ_COL.equalsIgnoreCase(fields[i])) {
                            vectorTable.getColumn(i).appendLong(-1L);
                            continue;
                        }
                        int paimonIndex = fieldToPaimonIndex[i];
                        if (paimonIndex < 0) {
                            appendNull(i);
                            continue;
                        }
                        int projectedIndex = fieldToDataTypeIndex[i];
                        columnValue.setIdx(projectedIndex, types[i], paimonDataTypeList.get(projectedIndex));
                        appendData(i, columnValue);
                    }
                    rows++;
                    if (rows >= batchSize) {
                        return rows;
                    }
                }
                recordIterator.releaseBatch();
                recordIterator = reader.readBatch();
            }
        } catch (Exception e) {
            close();
            LOG.warn("Failed to get the next batch of paimon. "
                            + "split: {}, requiredFieldNames: {}, paimonAllFieldNames: {}, dataType: {}",
                    getSplit(), params.get("required_fields"), paimonAllFieldNames, paimonDataTypeList, e);
            throw new IOException(e);
        }
        return rows;
    }

    @Override
    protected int getNext() {
        try {
            return preExecutionAuthenticator.execute(this::readAndProcessNextBatch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    private void initTable() {
        if (params.containsKey("serialized_table")) {
            table = PaimonUtils.deserialize(params.get("serialized_table"));
        } else {
            PaimonTableCacheKey key = new PaimonTableCacheKey(ctlId, dbId, tblId,
                    paimonOptionParams, hadoopOptionParams, dbName, tblName);
            TableExt tableExt = PaimonTableCache.getTable(key);
            if (tableExt.getCreateTime() < lastUpdateTime) {
                LOG.warn("invalidate cache table:{}, localTime:{}, remoteTime:{}", key, tableExt.getCreateTime(),
                        lastUpdateTime);
                PaimonTableCache.invalidateTableCache(key);
                tableExt = PaimonTableCache.getTable(key);
            }
            this.table = tableExt.getTable();
        }
        paimonAllFieldNames = PaimonUtils.getFieldNames(this.table.rowType());
        if (LOG.isDebugEnabled()) {
            LOG.debug("paimonAllFieldNames:{}", paimonAllFieldNames);
        }
    }

    private String getChangeType(String rowKind) {
        if (!MIN_DELTA.equalsIgnoreCase(getStreamConsumeType())
                && !DIFF.equalsIgnoreCase(getIncrementalBetweenScanMode())) {
            return APPEND;
        }
        switch (rowKind) {
            case "+I":
                return APPEND;
            case "-D":
                return DELETE;
            case "-U":
                return UPDATE_BEFORE;
            case "+U":
                return UPDATE_AFTER;
            default:
                return APPEND;
        }
    }

    private String getPaimonRowKindValue(String rowKind) {
        return isPaimonStreamScan() ? getChangeType(rowKind) : rowKind;
    }

    private boolean shouldSkipRecord(String rowKind) {
        return isPaimonStreamScan()
                && APPEND_ONLY.equalsIgnoreCase(getStreamConsumeType())
                && !INSERT_ROW_KIND.equals(rowKind);
    }

    private boolean isPaimonStreamScan() {
        return Boolean.parseBoolean(params.get(DORIS_PAIMON_STREAM_READ));
    }

    private String getStreamConsumeType() {
        return params.getOrDefault(DORIS_PAIMON_STREAM_CONSUME_TYPE, "");
    }

    private String getIncrementalBetweenScanMode() {
        String mode = params.get(INCREMENTAL_BETWEEN_SCAN_MODE);
        if (mode == null) {
            mode = params.get(PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE);
        }
        return mode == null ? "" : mode;
    }

}

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

package org.apache.doris.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/*
 * AuditEvent contains all information about audit log info.
 * It should be created by AuditEventBuilder. For example:
 *
 *      AuditEvent event = new AuditEventBuilder()
 *          .setEventType(AFTER_QUERY)
 *          .setClientIp(xxx)
 *          ...
 *          .build();
 */
public class AuditEvent {
    public enum EventType {
        CONNECTION,
        DISCONNECTION,
        BEFORE_QUERY,
        AFTER_QUERY,
        LOAD_SUCCEED,
        STREAM_LOAD_FINISH
    }

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface AuditField {
        String value() default "";
    }

    public EventType type;

    // all fields which is about to be audit should be annotated by "@AuditField"
    // make them all "public" so that easy to visit.
    @JsonProperty("timestamp")
    @AuditField(value = "Timestamp")
    public long timestamp = -1;
    @JsonProperty("client_ip")
    @AuditField(value = "Client")
    public String clientIp = "";
    @JsonProperty("user")
    @AuditField(value = "User")
    public String user = "";
    @JsonProperty("ctl")
    @AuditField(value = "Ctl")
    public String ctl = "";
    @JsonProperty("db")
    @AuditField(value = "Db")
    public String db = "";
    @AuditField(value = "CommandType")
    public String commandType = "";
    @JsonProperty("state")
    @AuditField(value = "State")
    public String state = "";
    @AuditField(value = "ErrorCode")
    public int errorCode = 0;
    @JsonProperty("error_message")
    @AuditField(value = "ErrorMessage")
    public String errorMessage = "";
    @JsonProperty("duration_ms")
    @AuditField(value = "Time(ms)")
    public long queryTime = -1;
    @JsonProperty("scan_bytes")
    @AuditField(value = "ScanBytes")
    public long scanBytes = -1;
    @JsonProperty("scan_rows")
    @AuditField(value = "ScanRows")
    public long scanRows = -1;
    @JsonProperty("return_rows")
    @AuditField(value = "ReturnRows")
    public long returnRows = -1;
    @AuditField(value = "StmtId")
    public long stmtId = -1;
    @JsonProperty("query_id")
    @AuditField(value = "QueryId")
    public String queryId = "";
    @JsonProperty("is_query")
    @AuditField(value = "IsQuery")
    public boolean isQuery = false;
    @AuditField(value = "IsNereids")
    public boolean isNereids = false;
    @JsonProperty("fe_ip")
    @AuditField(value = "FeIp")
    public String feIp = "";
    @JsonProperty("sql")
    @AuditField(value = "Stmt")
    public String stmt = "";
    @AuditField(value = "CpuTimeMS")
    public long cpuTimeMs = -1;
    @AuditField(value = "ShuffleSendBytes")
    public long shuffleSendBytes = -1;
    @AuditField(value = "ShuffleSendRows")
    public long shuffleSendRows = -1;
    @AuditField(value = "SqlHash")
    public String sqlHash = "";
    @AuditField(value = "PeakMemoryBytes")
    public long peakMemoryBytes = -1;
    @AuditField(value = "SqlDigest")
    public String sqlDigest = "";
    @AuditField(value = "WorkloadGroup")
    public String workloadGroup = "";
    // note: newly added fields should be always before fuzzyVariables
    @AuditField(value = "FuzzyVariables")
    public String fuzzyVariables = "";
    @AuditField(value = "scanBytesFromLocalStorage")
    public long scanBytesFromLocalStorage = -1;
    @AuditField(value = "scanBytesFromRemoteStorage")
    public long scanBytesFromRemoteStorage = -1;
    @JsonProperty("log_id")
    @AuditField(value = "LogId")
    public String logId = "";
    @JsonProperty("cluster")
    public String cluster = "";
    @JsonProperty("profile")
    @AuditField(value = "Profile")
    public String profile = "";
    @JsonProperty("fingerprint")
    @AuditField(value = "Fingerprint")
    public String fingerprint = "";
    @JsonProperty("is_insert")
    @AuditField(value = "IsInsert")
    public boolean isInsert = false;
    @JsonProperty("min_max_partition_names")
    @AuditField(value = "MinMaxPartitionNames")
    public String minMaxPartitionNames = "";
    // currently only scan olap tables, use the name to be consistent with community
    @JsonProperty("queriedTablesAndViews")
    @AuditField(value = "queriedTablesAndViews")
    public String queriedTablesAndViews = "";

    public long pushToAuditLogQueueTime;

    public static class AuditEventBuilder {

        private AuditEvent auditEvent = new AuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new AuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setTimestamp(long timestamp) {
            auditEvent.timestamp = timestamp;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setCtl(String ctl) {
            auditEvent.ctl = ctl;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setState(String state) {
            auditEvent.state = state;
            return this;
        }

        public AuditEventBuilder setErrorCode(int errorCode) {
            auditEvent.errorCode = errorCode;
            return this;
        }

        public AuditEventBuilder setErrorMessage(String errorMessage) {
            auditEvent.errorMessage = errorMessage;
            return this;
        }

        public AuditEventBuilder setQueryTime(long queryTime) {
            auditEvent.queryTime = queryTime;
            return this;
        }

        public AuditEventBuilder setScanBytes(long scanBytes) {
            auditEvent.scanBytes = scanBytes;
            return this;
        }

        public AuditEventBuilder setCpuTimeMs(long cpuTimeMs) {
            auditEvent.cpuTimeMs = cpuTimeMs;
            return this;
        }

        public AuditEventBuilder setPeakMemoryBytes(long peakMemoryBytes) {
            auditEvent.peakMemoryBytes = peakMemoryBytes;
            return this;
        }

        public AuditEventBuilder setScanRows(long scanRows) {
            auditEvent.scanRows = scanRows;
            return this;
        }

        public AuditEventBuilder setReturnRows(long returnRows) {
            auditEvent.returnRows = returnRows;
            return this;
        }

        public AuditEventBuilder setStmtId(long stmtId) {
            auditEvent.stmtId = stmtId;
            return this;
        }

        public AuditEventBuilder setQueryId(String queryId) {
            auditEvent.queryId = queryId;
            return this;
        }

        public AuditEventBuilder setIsQuery(boolean isQuery) {
            auditEvent.isQuery = isQuery;
            return this;
        }

        public AuditEventBuilder setIsNereids(boolean isNereids) {
            auditEvent.isNereids = isNereids;
            return this;
        }

        public AuditEventBuilder setFeIp(String feIp) {
            auditEvent.feIp = feIp;
            return this;
        }

        public AuditEventBuilder setStmt(String stmt) {
            auditEvent.stmt = stmt;
            return this;
        }

        public AuditEventBuilder setSqlHash(String sqlHash) {
            auditEvent.sqlHash = sqlHash;
            return this;
        }

        public AuditEventBuilder setSqlDigest(String sqlDigest) {
            auditEvent.sqlDigest = sqlDigest;
            return this;
        }

        public AuditEventBuilder setFuzzyVariables(String variables) {
            auditEvent.fuzzyVariables = variables;
            return this;
        }

        public AuditEventBuilder setWorkloadGroup(String workloadGroup) {
            auditEvent.workloadGroup = workloadGroup;
            return this;
        }

        public AuditEventBuilder setCommandType(String commandType) {
            auditEvent.commandType = commandType;
            return this;
        }

        public AuditEventBuilder setScanBytesFromLocalStorage(long scanBytesFromLocalStorage) {
            auditEvent.scanBytesFromLocalStorage = scanBytesFromLocalStorage;
            return this;
        }

        public AuditEventBuilder setScanBytesFromRemoteStorage(long scanBytesFromRemoteStorage) {
            auditEvent.scanBytesFromRemoteStorage = scanBytesFromRemoteStorage;
            return this;
        }

        public AuditEventBuilder setLogId(String logId) {
            auditEvent.logId = logId;
            return this;
        }

        public AuditEventBuilder setProfile(String profile) {
            auditEvent.profile = profile;
            return this;
        }

        public AuditEventBuilder setCluster(String cluster) {
            auditEvent.cluster = cluster;
            return this;
        }

        public AuditEventBuilder setFingerprint(String fingerprint) {
            auditEvent.fingerprint = fingerprint;
            return this;
        }

        public AuditEventBuilder setIsInsert(boolean isInsert) {
            auditEvent.isInsert = isInsert;
            return this;
        }

        public AuditEventBuilder setMinMaxPartitionNames(String minMaxPartitionNames) {
            auditEvent.minMaxPartitionNames = minMaxPartitionNames;
            return this;
        }

        public AuditEventBuilder setQueriedTablesAndViews(String queriedTablesAndViews) {
            auditEvent.queriedTablesAndViews = queriedTablesAndViews;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}

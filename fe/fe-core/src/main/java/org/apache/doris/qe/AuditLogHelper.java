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

package org.apache.doris.qe;

import org.apache.doris.DorisFE;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.NativeInsertStmt;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.ValueList;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.FingerprintMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.FrontendOptions;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AuditLogHelper {

    private static final Logger LOG = LogManager.getLogger(AuditLogHelper.class);

    /**
     * Add a new method to wrap original logAuditLog to catch all exceptions. Because write audit
     * log may write to a doris internal table, we may meet errors. We do not want this affect the
     * query process. Ignore this error and just write warning log.
     */
    public static void logAuditLog(ConnectContext ctx, String origStmt, StatementBase parsedStmt,
            org.apache.doris.proto.Data.PQueryStatistics statistics, boolean printFuzzyVariables, String logId) {
        try {
            origStmt = handleStmt(origStmt, parsedStmt);
            logAuditLogImpl(ctx, origStmt, parsedStmt, statistics, printFuzzyVariables, logId);
        } catch (Throwable t) {
            LOG.warn("Failed to write audit log.", t);
        }
    }

    /**
     * Truncate sql and if SQL is in the following situations, count the number of rows:
     * <ul>
     * <li>{@code insert into tbl values (1), (2), (3)}</li>
     * </ul>
     * The final SQL will be:
     * {@code insert into tbl values (1), (2 ...}
     */
    public static String handleStmt(String origStmt, StatementBase parsedStmt) {
        if (origStmt == null) {
            return null;
        }
        // 1. handle insert statement first
        Optional<String> res = handleInsertStmt(origStmt, parsedStmt);
        if (res.isPresent()) {
            return res.get();
        }

        // 2. handle other statement
        int maxLen = GlobalVariable.auditPluginMaxSqlLength;
        origStmt = truncateByBytes(origStmt, maxLen, " ... /* truncated. audit_plugin_max_sql_length=" + maxLen
                + " */");
        return origStmt.replace("\n", "\\n")
                .replace("\t", "\\t")
                .replace("\r", "\\r");
    }

    private static Optional<String> handleInsertStmt(String origStmt, StatementBase parsedStmt) {
        int rowCnt = 0;
        // old planner
        if (parsedStmt instanceof NativeInsertStmt) {
            QueryStmt queryStmt = ((NativeInsertStmt) parsedStmt).getQueryStmt();
            if (queryStmt instanceof SelectStmt) {
                ValueList list = ((SelectStmt) queryStmt).getValueList();
                if (list != null && list.getRows() != null) {
                    rowCnt = list.getRows().size();
                }
            }
        }
        // nereids planner
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlan plan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            if (plan instanceof InsertIntoTableCommand) {
                LogicalPlan query = ((InsertIntoTableCommand) plan).getLogicalQuery();
                if (query instanceof UnboundTableSink) {
                    rowCnt = countValues(query.children());
                }
            }
        }
        if (rowCnt > 0) {
            // This is an insert statement.
            int maxLen = Math.max(0,
                    Math.min(GlobalVariable.auditPluginMaxInsertStmtLength, GlobalVariable.auditPluginMaxSqlLength));
            origStmt = truncateByBytes(origStmt, maxLen, " ... /* total " + rowCnt
                    + " rows, truncated. audit_plugin_max_insert_stmt_length=" + maxLen + " */");
            origStmt = origStmt.replace("\n", "\\n")
                    .replace("\t", "\\t")
                    .replace("\r", "\\r");
            return Optional.of(origStmt);
        } else {
            return Optional.empty();
        }
    }

    private static String truncateByBytes(String str, int maxLen, String suffix) {
        // use `getBytes().length` to get real byte length
        if (maxLen >= str.getBytes().length) {
            return str;
        }
        Charset utf8Charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = utf8Charset.newDecoder();
        byte[] sb = str.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(sb, 0, maxLen);
        CharBuffer charBuffer = CharBuffer.allocate(maxLen);
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(buffer, charBuffer, true);
        decoder.flush(charBuffer);
        return new String(charBuffer.array(), 0, charBuffer.position()) + suffix;
    }

    /**
     * When SQL is in the following situations, count the number of rows:
     * <ul>
     * <li>{@code insert into tbl values (1), (2), (3)}</li>
     * </ul>
     */
    private static int countValues(List<Plan> children) {
        if (children == null) {
            return 0;
        }
        int cnt = 0;
        for (Plan child : children) {
            if (child instanceof UnboundOneRowRelation) {
                cnt++;
            } else if (child instanceof LogicalInlineTable) {
                cnt += ((LogicalInlineTable) child).getConstantExprsList().size();
            } else if (child instanceof LogicalUnion) {
                cnt += countValues(child.children());
            }
        }
        return cnt;
    }

    private static void logAuditLogImpl(ConnectContext ctx, String origStmt, StatementBase parsedStmt,
            org.apache.doris.proto.Data.PQueryStatistics statistics, boolean printFuzzyVariables, String logId) {
        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();
        CatalogIf catalog = ctx.getCurrentCatalog();

        AuditEventBuilder auditEventBuilder = ctx.getAuditEventBuilder();
        // ATTN: MUST reset, otherwise, the same AuditEventBuilder instance will be used in the next query.
        auditEventBuilder.reset();
        auditEventBuilder
                .setTimestamp(ctx.getStartTime())
                .setClientIp(ctx.getClientIP())
                .setUser(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser()))
                .setSqlHash(ctx.getSqlHash())
                .setEventType(EventType.AFTER_QUERY)
                .setCtl(catalog == null ? InternalCatalog.INTERNAL_CATALOG_NAME : catalog.getName())
                .setDb(ClusterNamespace.getNameFromFullName(ctx.getDatabase()))
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getState().getErrorCode() == null ? 0 : ctx.getState().getErrorCode().getCode())
                .setErrorMessage((ctx.getState().getErrorMessage() == null ? "" :
                        ctx.getState().getErrorMessage().replace("\n", " ").replace("\t", " ")))
                .setQueryTime(elapseMs)
                .setScanBytes(statistics == null ? 0 : statistics.getScanBytes())
                .setScanRows(statistics == null ? 0 : statistics.getScanRows())
                .setCpuTimeMs(statistics == null ? 0 : statistics.getCpuMs())
                .setPeakMemoryBytes(statistics == null ? 0 : statistics.getMaxPeakMemoryBytes())
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setQueryId(ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()))
                .setWorkloadGroup(ctx.getWorkloadGroupName())
                .setFuzzyVariables(!printFuzzyVariables ? "" : ctx.getSessionVariable().printFuzzyVariables())
                .setCommandType(ctx.getCommand().toString())
                .setCluster(DorisFE.CLUSTER)
                .setLogId(logId);

        // construct TOS profile url
        // we maybe fail to get TOS profile url because of sending failed.
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().enableProfile()) {
            long timeThreshold = ConnectContext.get().getSessionVariable().getReportQueryTimeThreshold();
            if (Config.audit_log_enable_send_profile_to_tos
                    && (elapseMs >= Math.max(timeThreshold, Config.audit_log_send_profile_min_time_ms)
                    || (ConnectContext.get() != null
                    && ConnectContext.get().getSessionVariable().isForceSendProfile()))) {
                StringBuilder profileUrl = new StringBuilder(Config.audit_log_send_profile_url_prefix);
                profileUrl.append(Config.audit_log_profile_tos_bucket);
                profileUrl.append("/");
                profileUrl.append(ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()));
                auditEventBuilder.setProfile(profileUrl.toString());
            }
        }

        if (Config.enable_fingerprint_metrics) {
            // compatible with old parser
            boolean isQuery = parsedStmt instanceof Queriable;
            boolean isInsert = parsedStmt instanceof InsertStmt;
            boolean isLogicalPlan = false;
            if (parsedStmt instanceof LogicalPlanAdapter) {
                isLogicalPlan = true;
                LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
                isQuery = !(logicalPlan instanceof Command);
                isInsert = (logicalPlan instanceof InsertIntoTableCommand)
                        || (logicalPlan instanceof BatchInsertIntoTableCommand)
                        || (logicalPlan instanceof InsertOverwriteTableCommand);
            }
            if (isQuery || isInsert) {
                String digest = "";
                if (isLogicalPlan) {
                    digest = ((LogicalPlanAdapter) parsedStmt).toDigest();
                } else if (isQuery) {
                    digest = ((Queriable) parsedStmt).toDigest();
                } else {
                    // do not compatible with old insert stmt
                }
                String sqlDigest = DigestUtils.md5Hex(digest);
                auditEventBuilder.setFingerprint(sqlDigest);
                auditEventBuilder.setIsInsert(isInsert);
            }
        }

        if (ctx.getExecutor() != null && ctx.getExecutor().planner() != null
                && ctx.getExecutor().planner() instanceof NereidsPlanner
                && !ctx.getSessionVariable().internalSession) {
            NereidsPlanner nereidsPlanner = (NereidsPlanner) ctx.getExecutor().planner();
            String tables = "[" + nereidsPlanner.getScanNodes().stream()
                    .filter(s -> s instanceof OlapScanNode)
                    .map(s -> ((OlapScanNode) s).getOlapTable().getQualifiedName())
                    .distinct()
                    .sorted()
                    .collect(Collectors.joining(",")) + "]";
            auditEventBuilder.setQueriedTablesAndViews(tables);
        }

        if (ctx.getState().isQuery()) {
            if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                MetricRepo.COUNTER_QUERY_ALL.increase(1L);
                MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(ctx.getQualifiedUser()).increase(1L);
            }
            if (ctx.getState().getStateType() == MysqlStateType.ERR
                    && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                // err query
                if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                    MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                    MetricRepo.USER_COUNTER_QUERY_ERR.getOrAdd(ctx.getQualifiedUser()).increase(1L);
                }
            } else if (ctx.getState().getStateType() == MysqlStateType.OK
                    || ctx.getState().getStateType() == MysqlStateType.EOF) {
                // ok query
                if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                    MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                }

                if (elapseMs > Config.qe_slow_log_ms) {
                    String sqlDigest = DigestUtils.md5Hex(((Queriable) parsedStmt).toDigest());
                    auditEventBuilder.setSqlDigest(sqlDigest);
                }
            }
            if (Config.enable_audit_log_partition_level_stats) {
                ctx.getAuditEventBuilder().setMinMaxPartitionNames(ctx.getMinMaxPartitionNames());
            }
            auditEventBuilder.setIsQuery(true)
                    .setScanBytesFromLocalStorage(
                            statistics == null ? 0 : statistics.getScanBytesFromLocalStorage())
                    .setScanBytesFromRemoteStorage(
                            statistics == null ? 0 : statistics.getScanBytesFromRemoteStorage());
        } else {
            auditEventBuilder.setIsQuery(false);
        }
        auditEventBuilder.setIsNereids(ctx.getState().isNereids);

        auditEventBuilder.setFeIp(FrontendOptions.getLocalHostAddress());

        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            auditEventBuilder.setStmt(parsedStmt.toSql());
        } else {
            auditEventBuilder.setStmt(origStmt);
        }
        if (!Env.getCurrentEnv().isMaster()) {
            if (ctx.executor != null && ctx.executor.hasForwardedToMaster()) {
                auditEventBuilder.setState(ctx.executor.getProxyStatus());
                int proxyStatusCode = ctx.executor.getProxyStatusCode();
                if (proxyStatusCode != 0) {
                    auditEventBuilder.setErrorCode(proxyStatusCode);
                    auditEventBuilder.setErrorMessage(ctx.executor.getProxyErrMsg());
                }
            }
        }
        if (ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE && ctx.getState().getErrorCode() == null) {
            auditEventBuilder.setState(String.valueOf(MysqlStateType.OK));
        }
        AuditEvent event = auditEventBuilder.build();
        if (ctx.getCommand() == MysqlCommand.COM_STMT_EXECUTE) {
            if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                if (Config.enable_fingerprint_metrics) {
                    // execute command use special metric registry
                    // because if users do not use prepare/execute, the fingerprint will be the same
                    // we should distinguish these calls
                    FingerprintMetric.reportExecuteCommandFingerprint(event);
                }
                boolean ok = ((ctx.getState().getStateType() == MysqlStateType.OK)
                        || (ctx.getState().getStateType() == MysqlStateType.EOF));
                if (ok) {
                    MetricRepo.EXECUTE_CMD_REQUEST_OK.increase(1L);
                } else {
                    MetricRepo.EXECUTE_CMD_REQUEST_ERR.increase(1L);
                }
            }
        }
        if (ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                boolean ok = (ctx.getState().getErrorCode() == null)
                        || (ctx.getState().getStateType() == MysqlStateType.OK)
                        || (ctx.getState().getStateType() == MysqlStateType.EOF);
                if (ok) {
                    MetricRepo.PREPARE_CMD_REQUEST_OK.increase(1L);
                } else {
                    MetricRepo.PREPARE_CMD_REQUEST_ERR.increase(1L);
                }
            }
        }
        if (ctx.getCommand() == MysqlCommand.COM_STMT_EXECUTE
                && !ctx.getSessionVariable().isEnablePreparedStmtAuditLog()) {
            return;
        }
        Env.getCurrentEnv().getWorkloadRuntimeStatusMgr().submitFinishQueryToAudit(event);
        if (LOG.isDebugEnabled()) {
            LOG.debug("submit audit event: {}", event.queryId);
        }
    }
}


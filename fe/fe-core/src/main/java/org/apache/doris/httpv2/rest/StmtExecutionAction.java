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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InlineViewRef;
import org.apache.doris.analysis.LateralViewRef;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SetOperationStmt.SetOperand;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * For execute stmt or get create table stmt via http
 */
@RestController
public class StmtExecutionAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(StmtExecutionAction.class);
    private static StatementSubmitter stmtSubmitter = new StatementSubmitter();
    private static final String  NEW_LINE_PATTERN = "[\n\r]";

    private static final String NEW_LINE_REPLACEMENT = " ";

    private static final long DEFAULT_ROW_LIMIT = 1000;
    private static final long MAX_ROW_LIMIT = 10000;

    /**
     * Execute a SQL.
     * Request body:
     * {
     * "is_sync": 1,   // optional
     * "limit" : 1000  // optional
     * "stmt" : "select * from tbl1"   // required
     * }
     */
    @RequestMapping(path = "/api/query/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object executeSQL(@PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response, @RequestBody String body) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        ActionAuthorizationInfo authInfo = checkWithCookie(request, response, false);
        String fullDbName = getFullDbName(dbName);
        if (Config.enable_all_http_auth) {
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.ADMIN);
        }

        if (ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ns = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        Type type = new TypeToken<StmtRequestBody>() {
        }.getType();
        StmtRequestBody stmtRequestBody = new Gson().fromJson(body, type);

        if (Strings.isNullOrEmpty(stmtRequestBody.stmt)) {
            return ResponseEntityBuilder.badRequest("Missing statement request body");
        }
        LOG.info("stmt: {}, isSync:{}, limit: {}", stmtRequestBody.stmt, stmtRequestBody.is_sync,
                stmtRequestBody.limit);

        ConnectContext.get().changeDefaultCatalog(ns);
        ConnectContext.get().setDatabase(fullDbName);

        String streamHeader = request.getHeader("X-Doris-Stream");
        boolean isStream = !("false".equalsIgnoreCase(streamHeader));
        return executeQuery(authInfo, stmtRequestBody.is_sync, stmtRequestBody.limit, stmtRequestBody,
                            response, isStream);
    }


    /**
     * Get all create table stmt of a SQL
     *
     * @param ns
     * @param dbName
     * @param request
     * @param response
     * @param sql plain text of sql
     * @return plain text of create table stmts
     */
    @RequestMapping(path = "/api/query_schema/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object querySchema(@PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response, @RequestBody String sql) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        checkWithCookie(request, response, false);

        if (ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ns = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        if (StringUtils.isNotBlank(sql)) {
            sql = sql.replaceAll(NEW_LINE_PATTERN, NEW_LINE_REPLACEMENT);
        }
        LOG.info("sql: {}", sql);
        ConnectContext.get().changeDefaultCatalog(ns);
        ConnectContext.get().setDatabase(getFullDbName(dbName));
        return getSchema(sql);
    }

    /**
     * Execute a query
     *
     * @param authInfo
     * @param isSync
     * @param limit
     * @param stmtRequestBody
     * @return
     */
    private ResponseEntity executeQuery(ActionAuthorizationInfo authInfo, boolean isSync, long limit,
            StmtRequestBody stmtRequestBody, HttpServletResponse response, boolean isStream) {
        StatementSubmitter.StmtContext stmtCtx = new StatementSubmitter.StmtContext(stmtRequestBody.stmt,
                authInfo.fullUserName, authInfo.password, limit, isStream, response);
        Future<ExecutionResultSet> future = stmtSubmitter.submit(stmtCtx);

        if (isSync) {
            try {
                ExecutionResultSet resultSet = future.get();
                // if use stream response, we not need to response an object.
                if (isStream) {
                    return null;
                }
                return ResponseEntityBuilder.ok(resultSet.getResult());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("failed to execute stmt", e);
                return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
            }
        } else {
            return ResponseEntityBuilder.okWithCommonError("Not support async query execution");
        }
    }

    @NotNull
    private String getSchema(String sql) {
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase stmt = null;
        try {
            stmt = SqlParserUtils.getStmt(parser, 0);
            if (!(stmt instanceof QueryStmt)) {
                return "Only support query stmt";
            }
            Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
            QueryStmt queryStmt = (QueryStmt) stmt;
            Map<Long, TableIf> tableMap = Maps.newHashMap();
            Set<String> parentViewNameSet = Sets.newHashSet();
            queryStmt.getTables(analyzer, true, tableMap, parentViewNameSet);

            List<String> createStmts = Lists.newArrayList();
            for (TableIf tbl : tableMap.values()) {
                List<String> createTableStmts = Lists.newArrayList();
                Env.getDdlStmt(tbl, createTableStmts, null, null, false, true, -1L);
                if (!createTableStmts.isEmpty()) {
                    createStmts.add(createTableStmts.get(0));
                }
            }
            return Joiner.on("\n\n").join(createStmts);
        } catch (Exception e) {
            return "Error:" + e.getMessage();
        }
    }

    @RequestMapping(path = "/api/query_schema_detail/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object queryQuerySchemaDetail(@PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody String stmtBody) {
        checkWithCookie(request, response, false);

        if (ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ns = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        Type type = new TypeToken<StmtRequestBody>() {
        }.getType();
        StmtRequestBody stmtRequestBody = new Gson().fromJson(stmtBody, type);
        if (Strings.isNullOrEmpty(stmtRequestBody.stmt)) {
            return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.BAD_REQUEST,
                    "Missing statement request body");
        }
        LOG.debug("sql: {}", stmtRequestBody.stmt);

        ConnectContext.get().changeDefaultCatalog(ns);
        if (!"_test".equalsIgnoreCase(dbName)) {
            ConnectContext.get().setDatabase(getFullDbName(dbName));
        }
        return getQuerySchemaDetail(stmtRequestBody.stmt);
    }

    @NotNull
    private Object getQuerySchemaDetail(String sql) {
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase stmt = null;
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        result.put("data", data);
        try {
            stmt = SqlParserUtils.getStmt(parser, 0);
            if (!(stmt instanceof QueryStmt)) {
                return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.NOT_SUPPORT_ERROR,
                        "Only support query stmt");
            }
            QueryStmt queryStmt = (QueryStmt) stmt;
            if (queryStmt.hasOutFileClause()) {
                return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.NOT_SUPPORT_ERROR,
                        "Query stmt has outfile clause");
            }
            Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
            queryStmt.analyze(analyzer);
            result.put("meta", getQuerySchemaColumns(queryStmt, false));
            Set<String> tables = new HashSet<>();
            data.put("details", getQuerySchemaDetail(queryStmt, tables));
            data.put("tables", tables);
            data.put("syntax_correct", "1");
            data.put("syntax_message", "success");
        } catch (UserException e) {
            data.put("syntax_correct", "0");
            data.put("syntax_message", e.getMessage());
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return ResponseEntityBuilder.ok(result);
    }

    private List<Map<String, Object>> getQuerySchemaDetail(QueryStmt queryStmt, Set<String> tables) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (queryStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            for (TableRef tableRef : selectStmt.getTableRefs()) {
                result.addAll(getQuerySchemaTableRefDetail(queryStmt, tableRef, tables));
            }
            if (selectStmt.getWhereClause() != null) {
                List<Subquery> subqueries = new ArrayList<>();
                selectStmt.getWhereClause().collect(Subquery.class, subqueries);
                for (Subquery subquery : subqueries) {
                    getQuerySchemaDetail(subquery.getStatement(), tables);
                }
            }
        } else if (queryStmt instanceof SetOperationStmt) {
            SetOperationStmt setOperationStmt = (SetOperationStmt) queryStmt;
            for (SetOperand setOperand : setOperationStmt.getOperands()) {
                result.addAll(getQuerySchemaDetail(setOperand.getQueryStmt(), tables));
            }
        }
        return result;
    }

    private List<Map<String, Object>> getQuerySchemaTableRefDetail(QueryStmt queryStmt, TableRef tableRef,
            Set<String> tables) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (tableRef instanceof BaseTableRef) {
            BaseTableRef table = (BaseTableRef) tableRef;
            Map<String, Object> tableInfo = new HashMap<>();
            if (table.getTable() instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table.getTable();
                tableInfo.put("table", wrapTableName(olapTable.getQualifiedName()));
                tableInfo.put("columns", getQuerySchemaColumns(queryStmt, true));
                result.add(tableInfo);
                tables.add(wrapTableName(olapTable.getQualifiedName()));
            }
            getQuerySchemaFilter(queryStmt, tableInfo);
        } else if (tableRef instanceof InlineViewRef) {
            InlineViewRef viewRef = (InlineViewRef) tableRef;
            if (viewRef.isLocalView()) {
                result.addAll(getQuerySchemaDetail(viewRef.getQueryStmt(), tables));
            } else {
                View view = viewRef.getView();
                Map<String, Object> tableInfo = new HashMap<>();
                tableInfo.put("table", wrapTableName(view.getQualifiedName()));
                tableInfo.put("columns", getQuerySchemaColumns(queryStmt, true));
                result.add(tableInfo);
                tables.add(wrapTableName(view.getQualifiedName()));
                getQuerySchemaFilter(queryStmt, tableInfo);
            }
        } else if (tableRef instanceof LateralViewRef) {
            LateralViewRef lateralViewRef = (LateralViewRef) tableRef;
            result.addAll(getQuerySchemaTableRefDetail(queryStmt, lateralViewRef.getRelatedTableRef(), tables));
        }
        return result;
    }

    private void getQuerySchemaFilter(QueryStmt queryStmt, Map<String, Object> tableInfo) {
        if (queryStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) queryStmt;
            if (selectStmt.getWhereClause() != null) {
                List<SlotRef> slots = new ArrayList<>();
                Set<String> filters = new HashSet<>();
                tableInfo.put("filters", filters);
                Expr.collectList(Collections.singletonList(selectStmt.getWhereClause()), SlotRef.class, slots);
                for (SlotRef slot : slots) {
                    filters.add(slot.getColumnName());
                }
            }
        }
    }

    private List<Map<String, Object>> getQuerySchemaColumns(QueryStmt queryStmt, boolean allowAlias) {
        List<Map<String, Object>> columns = new ArrayList<>();
        for (int i = 0; i < queryStmt.getColLabels().size(); i++) {
            String label = queryStmt.getColLabels().get(i);
            Expr expr = queryStmt.getResultExprs().get(i);
            Map<String, Object> column = new HashMap<>();
            column.put("name", label);
            column.put("type", expr.getType().toString());
            SlotDescriptor slotDescriptor = findSrcSlot(expr);
            if (slotDescriptor != null && (allowAlias || Objects.equals(slotDescriptor.getColumn().getName(), label))) {
                TupleDescriptor tupleDescriptor = slotDescriptor.getParent();
                TableIf table = tupleDescriptor.getTable();
                if (table instanceof OlapTable) {
                    column.put("table", wrapTableName(((OlapTable) table).getQualifiedName()));
                } else if (table instanceof InlineView) {
                    TableRef tableRef = tupleDescriptor.getRef();
                    InlineViewRef viewRef = (InlineViewRef) tableRef;
                    View view = viewRef.getView();
                    column.put("table", wrapTableName((view.getQualifiedName())));
                }
            }
            columns.add(column);
        }
        return columns;
    }

    private static String wrapTableName(String name) {
        if (name == null || name.contains(":")) {
            return name;
        }
        return SystemInfoService.DEFAULT_CLUSTER + ":" + name;
    }

    private static SlotDescriptor findSrcSlot(Expr expr) {
        SlotRef slotRef = expr.unwrapSlotRef(false);
        if (slotRef == null) {
            return null;
        }
        SlotDescriptor slotDesc = slotRef.getDesc();
        TableIf table = slotDesc.getParent().getTable();
        if (table instanceof OlapTable) {
            return slotDesc;
        }
        if (table instanceof InlineView && slotDesc.getParent().getRef() instanceof InlineViewRef) {
            InlineViewRef viewRef = (InlineViewRef) slotDesc.getParent().getRef();
            if (!viewRef.isLocalView()) {
                return slotDesc;
            }
        }
        if (slotDesc.getSourceExprs().size() == 1) {
            return findSrcSlot(slotDesc.getSourceExprs().get(0));
        }
        // No known source expr, or there are several source exprs meaning the slot is
        // has no single source table.
        return null;
    }

    private static class StmtRequestBody {
        public Boolean is_sync = true; // CHECKSTYLE IGNORE THIS LINE
        public Long limit = DEFAULT_ROW_LIMIT;
        public String stmt;
    }
}

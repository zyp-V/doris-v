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

import org.apache.doris.analysis.AlterDatabaseQuotaStmt;
import org.apache.doris.analysis.AlterDatabaseRename;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.DescribeStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
// import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * For get tables and operators of stmt via http
 */
@RestController
public class GetQueryTableAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(GetQueryTableAction.class);

    private enum Operator {
        SELECT,
        INSERT,
        ALTER,
        DROP,
        DELETE,
        CREATE,
        CREATE_NEW,
        SHOW,
        SET,
        INVALID_OPERATOR
    }

    /**
     * Get tables from a SQL.
     * Request body:
     * {
     * "stmt" : "select * from tbl1"
     * }
     */
    @RequestMapping(path = "/api/_get_query_table/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object getQueryTable(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody String stmtBody) throws DdlException {
        // 0. prepare

        // should get the ConnectContext (parse sql need)
        checkAuthWithCookie(request, response);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.BAD_REQUEST,
                "Only support 'default_cluster' now");
        }

        Type type = new TypeToken<StmtRequestBody>() {
        }.getType();
        GetQueryTableAction.StmtRequestBody stmtRequestBody = new Gson().fromJson(stmtBody, type);

        if (Strings.isNullOrEmpty(stmtRequestBody.stmt)) {
            return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.BAD_REQUEST,
                "Missing statement request body");
        }

        TableAnalyzer tableAnalyzer = new TableAnalyzer(getFullDb(dbName));
        /* Reason: the api support parse all sql (table is other cluster), so delete check Permission
        if (tableAnalyzer.dbIsExist(tableAnalyzer.defaultDb)) {
            ConnectContext.get().setDatabase(tableAnalyzer.defaultDb);
        } else {
            return ResponseEntityBuilder.okWithSpecifiedError(RestApiStatusCode.BAD_REQUEST,
                String.format("db [%s] didn't exist", tableAnalyzer.defaultDb));
        }
        */
        // 1. analyze the stmts is normal or not
        List<StatementBase> stmts = null;
        String originStmt = stmtRequestBody.stmt;

        // don't know hot to get stmt
        // if (ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
        //    try {
        //        stmts = new NereidsParser().parseSQL(originStmt);
        //    } catch (Exception e) {
        //        LOG.info(" Fallback to stale planner."
        //                + " Nereids cannot process this statement: \"{}\".", originStmt);
        //    }
        // }

        // stmts == null when Nereids cannot planner this query or Nereids is disabled.
        if (stmts == null) {
            try {
                stmts = SqlParserUtils.parseStmts(originStmt, ConnectContext.get());
            } catch (AnalysisException e) {
                return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage())).getHttpResponse();
            } catch (UserException e) {
                return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage())).getHttpResponse();
            } catch (Throwable e) {
                return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage())).getHttpResponse();
            }
        }

        List<String> origSingleStmtList = null;
        // if stmts.size() > 1, split originStmt to multi singleStmts
        if (stmts.size() > 1) {
            try {
                origSingleStmtList = SqlUtils.splitMultiStmts(originStmt);
            } catch (Exception ignore) {
                LOG.warn("Try to parse multi origSingleStmt failed, originStmt: \"{}\"", originStmt);
            }
        } else {
            origSingleStmtList = Collections.singletonList(originStmt);
        }

        for (int i = 0; i < stmts.size(); ++i) {
            Operator operator;
            // analyse stmt one by one
            StatementBase parsedStmt = stmts.get(i);
            boolean isSupport = true;
            if (parsedStmt == null) {
                return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                    "parsed statement is null")).getHttpResponse();
            } else if (parsedStmt instanceof UseStmt) {
                tableAnalyzer.setDefaultDb(getFullDb(((UseStmt) parsedStmt).getDatabase()));
                continue;
            } else if (parsedStmt instanceof ShowStmt) {
                operator = Operator.SHOW;
            } else if (parsedStmt instanceof SelectStmt || parsedStmt instanceof SetOperationStmt) {
                operator = Operator.SELECT;
            } else if (parsedStmt instanceof InsertStmt) {
                operator = Operator.INSERT;
            } else if (parsedStmt instanceof AlterTableStmt) {
                operator = Operator.ALTER;
            } else if (parsedStmt instanceof AlterDatabaseQuotaStmt || parsedStmt instanceof AlterDatabaseRename) {
                operator = Operator.ALTER;
            } else if (parsedStmt instanceof DropDbStmt) {
                operator = Operator.DROP;
            } else if (parsedStmt instanceof DropTableStmt) {
                operator = Operator.DROP;
            } else if (parsedStmt instanceof DeleteStmt) {
                operator = Operator.DELETE;
            } else if (parsedStmt instanceof CreateDbStmt || parsedStmt instanceof CreateTableStmt
                    || parsedStmt instanceof CreateTableLikeStmt) {
                operator = Operator.CREATE;
            } else if (parsedStmt instanceof SetStmt) {
                operator = Operator.SET;
            } else {
                isSupport = false;
                if (origSingleStmtList.get(i).trim().toLowerCase().indexOf("select") == 0) {
                    operator = Operator.SELECT;
                } else if (origSingleStmtList.get(i).trim().toLowerCase().indexOf("insert") == 0) {
                    operator = Operator.INSERT;
                } else if (origSingleStmtList.get(i).trim().toLowerCase().indexOf("alter") == 0) {
                    operator = Operator.ALTER;
                } else if (origSingleStmtList.get(i).trim().toLowerCase().indexOf("drop") == 0) {
                    operator = Operator.DROP;
                } else if (origSingleStmtList.get(i).trim().toLowerCase().indexOf("delete") == 0) {
                    operator = Operator.DELETE;
                } else if (origSingleStmtList.get(i).trim().toLowerCase().indexOf("create") == 0) {
                    operator = Operator.CREATE;
                } else {
                    operator = Operator.INVALID_OPERATOR;
                }
            }

            // 3. analyze and get table from the sql
            if (isSupport) {
                tableAnalyzer.setParsedStmt(parsedStmt);
                try {
                    tableAnalyzer.dispatch(operator);
                } catch (SpecifiedException e) {
                    return e.getHttpResponse();
                } catch (Exception e) {
                    return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage())).getHttpResponse();
                }
            } else {
                String message = ", please check the operator is supported or not";
                switch (operator) {
                    case INSERT:
                        message += "[now support insert into tbl xxx values | insert into tbl select xxx]";
                        break;
                    case SELECT:
                        message += "[now support select xxx]";
                        break;
                    case ALTER:
                        message += "[now support alter table | alter database quota | alter database rename]";
                        break;
                    case DROP:
                        message += "[now support drop table | drop database]";
                        break;
                    case DELETE:
                        message += "[now support delete from xx where]";
                        break;
                    case CREATE:
                        message += "[now support create db xx | create table xx (), "
                            + "create table as../like is not supported]";
                        break;
                    default:
                        message += "[the operator is invalid]";
                }
                return (new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "not support the Operator [" + operator + "]" + message)).getHttpResponse();
            }
        }
        try {
            return tableAnalyzer.getHttpResult();
        } catch (SpecifiedException e) {
            return e.getHttpResponse();
        }
    }

    protected class TableAnalyzer {
        private String defaultDb;
        private StatementBase parsedStmt;
        private Map<String, Set<Operator>> dbTableToOperator = new HashMap<>();

        TableAnalyzer(String defaultDb) {
            this.defaultDb = defaultDb;
        }

        boolean dbIsExist(String db) {
            if (!Env.getCurrentEnv().getCurrentCatalog().getDb(db).isPresent()) {
                return false;
            }
            return true;
        }

        List<Subquery> collectSubQuery(Expr root) {
            List<Subquery> subqueries = new ArrayList<>();
            for (Expr child : root.getChildren()) {
                if (child instanceof Subquery) {
                    subqueries.add((Subquery) child);
                } else {
                    subqueries.addAll(collectSubQuery(child));
                }
            }
            return subqueries;
        }

        void setParsedStmt(StatementBase parsedStmt) {
            this.parsedStmt = parsedStmt;
        }

        void setDefaultDb(String defaultDb) {
            this.defaultDb = defaultDb;
        }

        void dispatch(Operator operator) throws SpecifiedException {
            try {
                switch (operator) {
                    case SELECT:
                        handleSelect();
                        break;
                    case INSERT:
                        handleInsert();
                        break;
                    case ALTER:
                        handleAlter();
                        break;
                    case DROP:
                        handleDrop();
                        break;
                    case DELETE:
                        handleDelete();
                        break;
                    case CREATE:
                        handleCreate();
                        break;
                    case SHOW:
                        handleShow();
                        break;
                    case SET:
                        handleSet();
                        break;
                    default:
                        throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                                "not support the Operator [" + operator
                                    + "], please check the operator is supported or not");
                }
            } catch (SpecifiedException e) {
                throw e;
            } catch (Exception e) {
                throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage());
            }
        }

        void handleSelect() throws SpecifiedException {
            try {
                if (!(this.parsedStmt instanceof QueryStmt)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, "not query stmt");
                }
                ConnectContext context = new ConnectContext();
                context.setDatabase(this.defaultDb);
                Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), context);
                QueryStmt queryStmt = (QueryStmt) this.parsedStmt;
                Map<Long, TableIf> tableMap = Maps.newHashMap();
                Set<String> parentViewNameSet = Sets.newHashSet();
                queryStmt.getTables(analyzer, true, tableMap, parentViewNameSet);
                String db = "null";
                String table = "null";
                String completeName;
                for (TableIf tbl : tableMap.values()) {
                    db = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER,
                        tbl.getDatabase().getFullName());
                    table = tbl.getName();
                    completeName = db + "." + table;
                    if (!dbTableToOperator.containsKey(completeName)) {
                        Set<Operator> opSet = new HashSet<>();
                        dbTableToOperator.put(completeName, opSet);
                    }
                    dbTableToOperator.get(completeName).add(Operator.SELECT);
                }
            } catch (Exception e) {
                throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage());
            }
        }

        void handleInsert() throws SpecifiedException {
            String db = "null";
            String table = "null";
            String completeName;
            String statementType = "invalid";

            if (parsedStmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) parsedStmt;

                db = insertStmt.getDbName();
                table = insertStmt.getTableName();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [insert] -> insert into table but table is null");
                }

                StatementBase parserStmt = this.parsedStmt;
                StatementBase queryStmt = insertStmt.getQueryStmt();
                try {
                    setParsedStmt(queryStmt);
                    handleSelect();
                    setParsedStmt(parserStmt);
                } catch (Exception e) {
                    throw e;
                }

                statementType = "InsertStmt";
            }

            completeName = db + "." + table;
            if (!completeName.equals("null.null")) {
                if (!dbTableToOperator.containsKey(completeName)) {
                    Set<Operator> opSet = new HashSet<>();
                    dbTableToOperator.put(completeName, opSet);
                }
                dbTableToOperator.get(completeName).add(Operator.INSERT);
            } else {
                throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "operator [insert] -> table is null and db is null too, statementType: " + statementType);
            }
        }

        void handleAlter() throws SpecifiedException {
            String db = "null";
            String table = "null";
            String completeName;
            String statementType = "invalid";

            if (parsedStmt instanceof AlterTableStmt) {
                TableName tbl = ((AlterTableStmt) parsedStmt).getTbl();
                db = tbl.getDb();
                table = tbl.getTbl();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [alter] -> alter table but table is null");
                }
                statementType = "AlterTableStmt";
            } else if (parsedStmt instanceof AlterDatabaseQuotaStmt) {
                db = ((AlterDatabaseQuotaStmt) parsedStmt).getDbName();
                if (Strings.isNullOrEmpty(db)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [alter] -> alter database but db is null");
                } else {
                    db = getFullDb(db);
                }
                statementType = "AlterDatabaseQuotaStmt";
            } else if (parsedStmt instanceof AlterDatabaseRename) {
                db = ((AlterDatabaseRename) parsedStmt).getDbName();
                if (Strings.isNullOrEmpty(db)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [alter] -> alter database but db is null");
                } else {
                    db = getFullDb(db);
                }
                statementType = "AlterDatabaseRename";
            }

            completeName = db + "." + table;
            if (!completeName.equals("null.null")) {
                if (!dbTableToOperator.containsKey(completeName)) {
                    Set<Operator> opSet = new HashSet<>();
                    dbTableToOperator.put(completeName, opSet);
                }
                dbTableToOperator.get(completeName).add(Operator.ALTER);
            } else {
                throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "operator [alter] -> table is null and db is null too, statementType: " + statementType);
            }
        }

        void handleDrop() throws SpecifiedException {
            String db = "null";
            String table = "null";
            String completeName;
            String statementType = "invalid";

            if (parsedStmt instanceof DropDbStmt) {
                db = ((DropDbStmt) parsedStmt).getDbName();
                if (Strings.isNullOrEmpty(db)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [drop] -> drop database but db is null");
                } else {
                    db = getFullDb(db);
                }
                statementType = "DropDbStmt";
                return;
            } else if (parsedStmt instanceof DropTableStmt) {
                DropTableStmt dropTableStmt = ((DropTableStmt) parsedStmt);
                db = dropTableStmt.getDbName();
                table = dropTableStmt.getTableName();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [drop] -> drop table but table is null");
                }
                statementType = "DropTableStmt";
            }

            completeName = db + "." + table;
            if (!completeName.equals("null.null")) {
                if (!dbTableToOperator.containsKey(completeName)) {
                    Set<Operator> opSet = new HashSet<>();
                    dbTableToOperator.put(completeName, opSet);
                }
                dbTableToOperator.get(completeName).add(Operator.DROP);
            } else {
                throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "operator [drop] -> table is null and db is null too, statementType: " + statementType);
            }
        }

        void handleDelete() throws SpecifiedException {
            String db = "null";
            String table = "null";
            String completeName;
            String statementType = "invalid";

            if (parsedStmt instanceof DeleteStmt) {
                DeleteStmt deleteStmt = (DeleteStmt) parsedStmt;
                db = deleteStmt.getDbName();
                table = deleteStmt.getTableName();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [delete] -> delete table but table is null");
                }
                statementType = "DeleteStmt";
            }

            completeName = db + "." + table;
            if (!completeName.equals("null.null")) {
                if (!dbTableToOperator.containsKey(completeName)) {
                    Set<Operator> opSet = new HashSet<>();
                    dbTableToOperator.put(completeName, opSet);
                }
                dbTableToOperator.get(completeName).add(Operator.DELETE);
            } else {
                throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "operator [delete] -> table is null and db is null too, statementType: " + statementType);
            }
        }

        void handleCreate() throws SpecifiedException {
            String db = "null";
            String table = "null";
            String completeName;
            String statementType = "invalid";

            if (parsedStmt instanceof CreateDbStmt) {
                CreateDbStmt createDbStmt = (CreateDbStmt) parsedStmt;
                db = createDbStmt.getFullDbName();
                if (Strings.isNullOrEmpty(db)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [create] -> create database but db is null");
                } else {
                    db = getFullDb(db);
                }
                statementType = "CreateStmt";
                if (!createDbStmt.isSetIfNotExists()) {
                    Database database = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbNullable(db);
                    if (database != null) {
                        throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                            "operator [create] -> create database but db: " + db + " already exists");
                    }
                }
            } else if (parsedStmt instanceof CreateTableStmt) {
                CreateTableStmt createTableStmt = (CreateTableStmt) parsedStmt;
                db = createTableStmt.getDbName();
                table = createTableStmt.getTableName();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [create] -> create table but table is null");
                }
                if (!createTableStmt.isSetIfNotExists()) {
                    Database database = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbNullable(db);
                    if (database == null) {
                        throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                            "operator [create] -> create table but db: " + db + " not exists");
                    } else if (database.isTableExist(table)) {
                        throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                            "operator [create] -> create table but table: " + db + "." + table + " already exists");
                    }
                }
            } else if (parsedStmt instanceof CreateTableLikeStmt) {
                CreateTableLikeStmt createTableLikeStmt = (CreateTableLikeStmt) parsedStmt;
                // newly created table
                db = createTableLikeStmt.getDbName();
                table = createTableLikeStmt.getTableName();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                // existing table
                String sourceDb = createTableLikeStmt.getExistedDbName();
                String sourceTable = createTableLikeStmt.getExistedTableName();
                sourceDb = Strings.isNullOrEmpty(sourceDb) ? defaultDb : getFullDb(sourceDb);
                if (Strings.isNullOrEmpty(sourceTable)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [create] -> create table like but target table is null");
                }
                String completeNameSource = sourceDb + "." + sourceTable;
                if (!completeNameSource.equals("null.null")) {
                    if (!dbTableToOperator.containsKey(completeNameSource)) {
                        Set<Operator> opSet = new HashSet<>();
                        dbTableToOperator.put(completeNameSource, opSet);
                    }
                    dbTableToOperator.get(completeNameSource).add(Operator.CREATE_NEW);
                } else {
                    throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                        "operator [create] -> table is null and db is null too, statementType: " + statementType);
                }
                    // check source db & table
                    {
                        Database database = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbNullable(sourceDb);
                        if (database == null) {
                            throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                                "operator [create] -> create table but source db: " + db + " not exists");
                        } else if (!database.isTableExist(sourceTable)) {
                            throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                                "operator [create] -> create table but source table: " + db + "." + table
                                    + " not exists");
                        }
                    }
                if (!createTableLikeStmt.isIfNotExists()) {
                    // check target db & table
                    Database database = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbNullable(db);
                    if (database == null) {
                        throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                            "operator [create] -> create table but target db: " + db + " not exists");
                    } else if (database.isTableExist(table)) {
                        throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                            "operator [create] -> create table but target table: " + db + "." + table
                                + " already exists");
                    }
                }
            }

            completeName = db + "." + table;
            if (!completeName.equals("null.null")) {
                if (!dbTableToOperator.containsKey(completeName)) {
                    Set<Operator> opSet = new HashSet<>();
                    dbTableToOperator.put(completeName, opSet);
                }
                dbTableToOperator.get(completeName).add(Operator.CREATE);
            } else {
                throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "operator [Create] -> table is null and db is null too, statementType: " + statementType);
            }
        }

        void handleShow() throws SpecifiedException {
            String db = "null";
            String table = "null";
            String completeName;
            String statementType = "invalid";
            if (parsedStmt instanceof DescribeStmt) {
                DescribeStmt describeStmt = (DescribeStmt) parsedStmt;
                db = describeStmt.getDb();
                table = describeStmt.getTableName();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [describe] -> describe table but table is null");
                }

            } else if (parsedStmt instanceof ShowCreateTableStmt) {
                ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) parsedStmt;
                db = showCreateTableStmt.getDb();
                table = showCreateTableStmt.getDb();
                db = Strings.isNullOrEmpty(db) ? defaultDb : getFullDb(db);
                if (Strings.isNullOrEmpty(table)) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                        "operator [show] -> show create table but table is null");
                }
            } else { // ignore other type of show stmt
                return;
            }
            completeName = db + "." + table;
            if (!completeName.equals("null.null")) {
                if (!dbTableToOperator.containsKey(completeName)) {
                    Set<Operator> opSet = new HashSet<>();
                    dbTableToOperator.put(completeName, opSet);
                }
                dbTableToOperator.get(completeName).add(Operator.SHOW);
            } else {
                throw new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR,
                    "operator [Show] -> table is null and db is null too, statementType: " + statementType);
            }
        }

        void handleSet() throws SpecifiedException {
            // just ignore set
        }

        public ResponseEntity getHttpResult() throws SpecifiedException {
            List<Map<String, Object>> datas = new ArrayList<>();
            for (Map.Entry<String, Set<GetQueryTableAction.Operator>> entry : dbTableToOperator.entrySet()) {
                String[] res = entry.getKey().split("\\.");
                if (res.length != 2 || res[0] == null || res[0].isEmpty()
                        || res[1] == null || res[1].isEmpty()) {
                    throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, "db.table is wrong type");
                }

                List<String> actions = new ArrayList<>();
                for (GetQueryTableAction.Operator action : entry.getValue()) {
                    actions.add(action.toString());
                }

                // check db / table if valid
                if (!actions.contains(Operator.CREATE.toString())) {
                    String dbName = getFullDbName(res[0]);
                    Database database = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbNullable(dbName);
                    if (database != null) {
                        if (!database.isTableExist(res[1])) {
                            throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                                "operator [" + String.join(",", actions) + "] -> table " + res[1] + " not found");
                        }
                    } else {
                        throw new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR,
                            "operator [" + String.join(",", actions) + "] -> database " + res[0] + " not found");
                    }
                }

                Map<String, Object> data = new HashMap<>();
                data.put("Action", actions);
                data.put("Table", res[1].equals("null") ? null : res[1]);
                data.put("DataBase", res[0].equals("null") ? null : res[0]);
                datas.add(data);
            }
            return ResponseEntityBuilder.ok(datas);
        }
    }

    protected class SpecifiedException extends Exception {
        RestApiStatusCode errorCode;

        SpecifiedException(RestApiStatusCode errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }

        ResponseEntity getHttpResponse() {
            LOG.warn(this.getMessage());
            return ResponseEntityBuilder.okWithSpecifiedError(errorCode,
                "Failed to get table from sql: " + this.getMessage());
        }
    }

    protected static String getFullDb(String dbName) {
        if (dbName.contains("default_cluster:")) {
            LOG.warn("db_key shouldn't contains 'default_cluster:'");
        } else {
            dbName = "default_cluster:" + dbName;
        }
        return dbName;
    }

    private static class StmtRequestBody {
        public String stmt;
    }
}

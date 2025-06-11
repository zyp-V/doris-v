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
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * For get query schema of stmt via http
 */
@RestController
public class GetQuerySchemaAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(GetQuerySchemaAction.class);

    /**
     * Execute a SQL.
     * Request body:
     * {
     * "is_sync": 1,   // optional
     * "limit" : 1000  // optional
     * "stmt" : "select * from tbl1"   // required
     * }
     */
    @RequestMapping(path = "/api/query_output_schema/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object queryOutputSchema(@PathVariable(value = NS_KEY) String ns,
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
        LOG.info("sql: {}", stmtRequestBody.stmt);

        ConnectContext.get().changeDefaultCatalog(ns);
        ConnectContext.get().setDatabase(getFullDbName(dbName));
        return getOutputSchema(stmtRequestBody.stmt);
    }

    @NotNull
    private Object getOutputSchema(String sql) {
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        StatementBase stmt = null;
        try {
            stmt = SqlParserUtils.getStmt(parser, 0);
            if (!(stmt instanceof QueryStmt)) {
                return (new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR, "Only support query stmt"))
                        .getHttpResponse();
            }
            QueryStmt queryStmt = (QueryStmt) stmt;
            if (queryStmt.hasOutFileClause()) {
                return (new SpecifiedException(RestApiStatusCode.NOT_SUPPORT_ERROR, "Query stmt has outfile clause"))
                        .getHttpResponse();
            }
            Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
            queryStmt.analyze(analyzer);
            List<Map<String, Object>> datas = new ArrayList<>();
            try {
                for (int i = 0; i < queryStmt.getColLabels().size(); i++) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("name", queryStmt.getColLabels().get(i));
                    data.put("type", queryStmt.getResultExprs().get(i).getType().toSql());
                    datas.add(data);
                }
            } catch (Throwable e) {
                return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage()))
                        .getHttpResponse();
            }
            return ResponseEntityBuilder.ok(datas);
        } catch (Exception e) {
            return (new SpecifiedException(RestApiStatusCode.ANALYZE_ERROR, e.getMessage())).getHttpResponse();
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

    private static class StmtRequestBody {
        public String stmt;
    }
}

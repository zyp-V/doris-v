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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * SHOW STREAMS command.
 */
public class ShowStreamsCommand extends Command implements NoForward {
    private String db;
    private String catalog;
    private final String likePattern;
    private final String whereClause;

    public ShowStreamsCommand(String db, String catalog, String likePattern, String whereClause) {
        super(PlanType.SHOW_STREAMS);
        this.db = db;
        this.catalog = catalog;
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    private boolean isShowStreamsCaseSensitive() {
        if (Env.getLowerCaseTableNames(catalog) == 0) {
            return CaseSensibility.TABLE.getCaseSensibility();
        }
        return false;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = ctx.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                catalog = org.apache.doris.datasource.InternalCatalog.INTERNAL_CATALOG_NAME;
            }
        }
        Util.prohibitExternalCatalog(catalog, this.getClass().getSimpleName());

        if (whereClause != null) {
            throw new AnalysisException("WHERE clause is not supported for SHOW STREAMS");
        }
    }

    public ShowResultSetMetaData getMetaData() {
        return ShowResultSetMetaData.builder()
                .addColumn(new org.apache.doris.catalog.Column("Streams_in_" + db,
                        org.apache.doris.catalog.ScalarType.createVarchar(20)))
                .build();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        DatabaseIf<TableIf> dbIf = ctx.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(catalog)
                .getDbOrAnalysisException(db);

        PatternMatcher matcher = null;
        if (likePattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(likePattern, isShowStreamsCaseSensitive());
        }

        List<List<String>> rows = Lists.newArrayList();
        Set<Long> streamIds = Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(dbIf);
        for (Long streamId : streamIds) {
            Optional<TableIf> table = dbIf.getTable(streamId);
            if (!table.isPresent()) {
                continue;
            }
            if (matcher != null && !matcher.match(table.get().getName())) {
                continue;
            }
            if (table.get().isTemporary()) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                    catalog, dbIf.getFullName(), table.get().getName(), PrivPredicate.SHOW)) {
                continue;
            }
            rows.add(Lists.newArrayList(table.get().getName()));
        }

        rows.sort(Comparator.comparing(x -> x.get(0)));
        executor.sendResultSet(new ShowResultSet(getMetaData(), rows));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}

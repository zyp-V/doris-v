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

package org.apache.doris.common.util;

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.Lists;

import java.util.List;

public class RowStoreOnlyUtil {
    public static final String ROW_STORE_ONLY_COMPLEX_QUERY_ERROR_MSG =
            "row_store_only table does not support queries with aggregate or join";

    public static boolean shouldBlockComplexQuery(SelectStmt selectStmt) {
        if (!Config.enable_row_store_only_complex_query_block) {
            return false;
        }
        boolean hasAggregateOrJoin = selectStmt.getAggInfo() != null
                || selectStmt.getTableRefs().size() > 1;
        if (!hasAggregateOrJoin) {
            return false;
        }

        List<TableRef> tblRefs = Lists.newArrayList();
        selectStmt.collectTableRefs(tblRefs);
        boolean hasRowStoreOnlyOlapTable = tblRefs.stream()
                .map(TableRef::getTable)
                .filter(table -> table instanceof OlapTable)
                .map(table -> (OlapTable) table)
                .anyMatch(OlapTable::rowStoreOnly);
        return hasRowStoreOnlyOlapTable;
    }

    public static boolean shouldBlockComplexQuery(Plan plan) {
        if (!Config.enable_row_store_only_complex_query_block) {
            return false;
        }
        boolean hasRowStoreOnlyOlapScan = plan.collect(OlapScan.class::isInstance).stream()
                .map(OlapScan.class::cast)
                .anyMatch(scan -> scan.getTable().rowStoreOnly());
        if (!hasRowStoreOnlyOlapScan) {
            return false;
        }

        boolean hasAggregateOrJoin = plan.anyMatch(node -> node instanceof LogicalAggregate
                || node instanceof LogicalJoin);
        return hasAggregateOrJoin;
    }
}

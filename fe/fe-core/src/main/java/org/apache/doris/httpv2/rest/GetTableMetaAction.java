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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.SystemInfoService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GetTableMetaAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(GetTableMetaAction.class);


    @RequestMapping(path = "/api/_get_table_meta/{" + NS_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}",
            method = RequestMethod.GET)
    public Object execute(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            HttpServletRequest request, HttpServletResponse response) throws DdlException {

        try {
            if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
                return ResponseEntityBuilder.internalError("Bad Request, Only support 'default_cluster' now");
            }

            dbName = getFullDbName(dbName);
            Database database = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbNullable(dbName);
            Table table = null;

            if (database != null) {
                table = database.getTableNullable(tblName);
            }

            if (table == null) {
                LOG.warn("get table meta failed, can't find the table: " + tblName);
                return ResponseEntityBuilder.internalError("get table meta failed, can't find the table: " + tblName);
            }

            if (table instanceof OlapTable) {
                Object result = getMetaFromOlapTable((OlapTable) table, dbName);
                return ResponseEntityBuilder.ok(result);
            } else {
                LOG.warn("get table meta failed, table [" + tblName
                        + "]isn't olapTable, now only get meta information from olapTable");
                return ResponseEntityBuilder.internalError("get table meta failed, table [" + tblName
                        + "]isn't olapTable, now only get meta information from olapTable");

            }
        } catch (Exception e) {
            LOG.warn("_get_table_meta failed ns: {}, db: {}, table: {}", ns, dbName, tblName, e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    public String getFullDbName(String dbName) {
        String fullDbName = dbName;
        String clusterName = ClusterNamespace.getNameFromFullName(fullDbName);
        if (clusterName == null) {
            fullDbName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName);
        }
        return fullDbName;
    }

    public static Object getMetaFromOlapTable(OlapTable table, String dbName) {
        Map<String, Object> data = new HashMap<>();
        data.put("type", table.getType());
        data.put("cluster_name", System.getenv("DORIS_CLUSTER"));
        data.put("name", table.getName());
        data.put("db", dbName);
        data.put("keysType", table.getKeysType().toSql());

        List<Map<String, Object>> schema = new ArrayList<>();
        for (Column column : table.getFullSchema()) {
            String colJson = GsonUtils.GSON.toJson(column);
            Map<String, Object> colMap = GsonUtils.GSON.fromJson(colJson, Map.class);
            colMap.put("type", column.getType().toSql());
            if (!colMap.containsKey("defaultValue")) {
                colMap.put("defaultValue", null);
            }
            if (!colMap.containsKey("aggregationType")) {
                colMap.put("aggregationType", null);
            }
            schema.add(colMap);
        }
        data.put("schema", schema);

        Map<String, Object> distributionInfo = new HashMap<>();
        Map<String, Object> info = new HashMap<>();
        DistributionInfo defaultDistributionInfo = table.getDefaultDistributionInfo();
        distributionInfo.put("typeStr", defaultDistributionInfo.getType().name());
        if (defaultDistributionInfo instanceof RandomDistributionInfo) {
            RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) defaultDistributionInfo;
            info.put("bucketNum", randomDistributionInfo.getBucketNum());
        } else if (defaultDistributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) defaultDistributionInfo;
            info.put("bucketNum", hashDistributionInfo.getBucketNum());
            List<String> distributionColumns = new ArrayList<>();
            for (Column col : hashDistributionInfo.getDistributionColumns()) {
                distributionColumns.add(col.getName());
            }
            info.put("distributionColumns", distributionColumns);
        } else {
            LOG.warn("get table meta failed, only support handle distribute type : RANDOM, HASH");
            throw new RuntimeException("get table meta failed, only support handle distribute type : RANDOM, HASH");
        }
        distributionInfo.put("info", info);
        data.put("distributionInfo", distributionInfo);

        Map<String, Object> partitionInfo = new HashMap<>();
        partitionInfo.put("partitionType", table.getPartitionInfo().getType().name());
        PartitionInfo defaultPartitionInfo = table.getPartitionInfo();
        if (defaultPartitionInfo instanceof RangePartitionInfo) {
            List<String> partitionColumns = new ArrayList<>();
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) defaultPartitionInfo;
            for (Column col : rangePartitionInfo.getPartitionColumns()) {
                partitionColumns.add(col.getName());
            }
            partitionInfo.put("partitionColumns", partitionColumns);
        } else if (defaultPartitionInfo instanceof SinglePartitionInfo) {
            // do noting
        } else {
            LOG.warn("get table meta failed, only support handle partition type : RANGE, UNPARTITIONED");
            return new RuntimeException(
                "get table meta failed, only support handle partition type : RANGE, UNPARTITIONED");
        }
        data.put("partitionInfo", partitionInfo);

        String indexesJson = GsonUtils.GSON.toJson(table.getTableIndexes());
        data.put("tableIndex", GsonUtils.GSON.fromJson(indexesJson, Map.class));

        data.put("tableProperties", table.getTableProperty().getProperties());
        data.put("hasSequenceCol", table.hasSequenceCol());
        if (table.getSequenceType() == null) {
            data.put("sequenceType", null);
        } else {
            data.put("sequenceType", table.getSequenceType().toString());
        }
        return data;
    }
}

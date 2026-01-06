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

package org.apache.doris.datasource.property.constants;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HMSProperties {
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    public static final String TYPE = "type";
    public static final String DLF_TYPE = "dlf";
    public static final String GLUE_TYPE = "glue";
    public static final String HIVE_VERSION = "hive.version";
    // required
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final List<String> REQUIRED_FIELDS = Collections.singletonList(HMSProperties.HIVE_METASTORE_URIS);
    public static final String  ENABLE_HMS_EVENTS_INCREMENTAL_SYNC = "hive.enable_hms_events_incremental_sync";
    public static final String  HMS_EVENTIS_BATCH_SIZE_PER_RPC = "hive.hms_events_batch_size_per_rpc";
    // used to connect to bytedance hive
    /*
    CREATE CATALOG hive_cn_token_3 PROPERTIES (
        "type"="hms",
        "hive.metastore.type" = "bytedance_hive",
        "region" = "CN",
        "staging_dir" = "path/to/staging/dir",
        "token"="xxx" // optional
    );
     */
    public static final String BYTEDANCE_TYPE = "bytedance_hive";
    public static final String DEFAULT_HIVE_HOME = "/opt/tiger/hive_deploy";
    public static final String HIVE_HOME = System.getenv("HIVE_HOME");

    public static final String DEFAULT_HADOOP_HOME = "/opt/tiger/yarn_deploy/hadoop";
    public static final String HADOOP_HOME = System.getenv("HADOOP_HOME");

    // ref: https://bytedance.larkoffice.com/wiki/wikcnevZWbviLwNWBGLP0LCSHIc
    public static ImmutableMap<String, String> REGION_CONF_MAP = ImmutableMap.<String, String>builder()
            .put("CN", "conf_sentry")
            .put("VA", "conf_i18n_sentry")
            .put("USWEST", "conf_uswest_sentry")
            .put("TTP", "conf_oci_sentry")
            .put("SG", "conf_sg_sentry")
            .put("JP", "conf_jp_lark_sentry")
            .put("EUPIPO", "conf_eupipo_sentry")
            .put("EUROPE", "conf_europe_sentry")
            .put("BOE", "conf_boe")
            .put("BOEI18N", "conf_boei18n")
            .put("COMPASS", "conf_gcp_sentry")
            .put("NO", "conf_norway_sentry")
            .put("PINNACLE", "conf_pinnacle_sentry")
            .build();

    public static boolean isBytedanceHive(Map<String, String> properties) {
        String type = properties.getOrDefault(TYPE, "");
        String hiveMetastoreType = properties.getOrDefault(HIVE_METASTORE_TYPE, "");
        return type.equalsIgnoreCase("hms") && hiveMetastoreType.equalsIgnoreCase(BYTEDANCE_TYPE);
    }
}

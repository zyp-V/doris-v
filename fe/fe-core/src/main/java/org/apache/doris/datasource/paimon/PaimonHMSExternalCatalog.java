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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.service.GdprService;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class PaimonHMSExternalCatalog extends PaimonExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonHMSExternalCatalog.class);
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            HMSProperties.HIVE_METASTORE_URIS
    );

    public PaimonHMSExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, resource, props, comment);
    }

    @Override
    protected void initLocalObjectsImpl() {
        super.initLocalObjectsImpl();
        catalogType = PAIMON_HMS;
        catalog = createCatalog();
    }

    @Override
    protected void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options) {
        options.put(PaimonProperties.PAIMON_CATALOG_TYPE, getPaimonCatalogType(catalogType));
        options.put(PaimonProperties.HIVE_METASTORE_URIS, properties.get(HMSProperties.HIVE_METASTORE_URIS));
        String region = properties.getOrDefault("region", "BOE");
        String hiveConfDir = getHivePath(region);
        LOG.info("hive-conf-dir: {}", hiveConfDir);
        options.put("hive-conf-dir", hiveConfDir);
        options.put("hadoop-conf-dir", getHadoopConfDir(region));
        String gdprToken = properties.get("token");
        if (Strings.isNullOrEmpty(gdprToken)) {
            gdprToken = GdprService.getGdprTokenFromENV();
            LOG.info("get zti token: {}", gdprToken);
        } else {
            LOG.info("token from catalog: {}", gdprToken);
        }
        options.put("ipc.client.custom_token", gdprToken);
        options.put("ipc.client.client-cache.enable", "false");
        if (properties.containsKey("hadoop.security.authentication")) {
            options.put("hadoop.security.authentication", properties.get("hadoop.security.authentication"));
        }
        options.put("fs.hdfs.impl.disable.cache", "true");
        options.put("fs.file.impl.disable.cache", "true");
        options.put("ipc.client.client-cache.enable", "false");
        options.put("dfs.slowRead.bytesPerSec.threshold", "5242880");
        options.put("dfs.client.hedged.read.enable", "true");
        options.put("hive-catalog.listTable.skip-exception", "true");
        options.put(HMSProperties.HIVE_VERSION, "1.2.2");
    }

    private String getHivePath(String region) {
        String confDir = HMSProperties.REGION_CONF_MAP.get(region.toUpperCase());
        String hiveHome = Strings.isNullOrEmpty(HMSProperties.HIVE_HOME)
                ? HMSProperties.DEFAULT_HIVE_HOME : HMSProperties.HIVE_HOME;
        String hiveConfDir = String.format("%s/%s/", hiveHome, confDir);
        return hiveConfDir;
    }

    private String getHadoopConfDir(String region) {
        String hadoopHome = Strings.isNullOrEmpty(HMSProperties.HADOOP_HOME)
                ? HMSProperties.DEFAULT_HADOOP_HOME : HMSProperties.HADOOP_HOME;
        String hadoopConfDir =  String.format("%s/conf/", hadoopHome);
        return hadoopConfDir;
    }

    @Override
    public void checkProperties() throws DdlException {
        String metastoreType = catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_TYPE, "");
        if (HMSProperties.BYTEDANCE_TYPE.equalsIgnoreCase(metastoreType)) {
            return;
        }
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }
}

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

package org.apache.doris.datasource.doris;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.RemoteDorisProperties;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RemoteDorisExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisExternalCatalog.class);

    private FeServiceClient client;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            RemoteDorisProperties.USER,
            RemoteDorisProperties.PASSWORD
    );

    /**
     * Default constructor for DorisExternalCatalog.
     */
    public RemoteDorisExternalCatalog(long catalogId, String name, String resource,
                                      Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.REMOTE_DORIS, comment);
        this.catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();

        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
        if (Config.isCloudMode()) {
            // TODO we not validate it in cloud mode, so currently not support it
            throw new DdlException("Cloud mode is not supported when "
                    + RemoteDorisProperties.USE_ARROW_FLIGHT + " is false");
        }
        String addresses = catalogProperty.getOrDefault(RemoteDorisProperties.FE_THRIFT_HOSTS, "");
        String psm = catalogProperty.getOrDefault(RemoteDorisProperties.FE_PSM, "");
        String portStr = catalogProperty.getOrDefault(RemoteDorisProperties.FE_THRIFT_PORT, "");
        if (StringUtils.isEmpty(addresses) && StringUtils.isEmpty(psm) && StringUtils.isEmpty(portStr)) {
            String property = RemoteDorisProperties.FE_THRIFT_HOSTS + " or " + RemoteDorisProperties.FE_PSM + ", "
                    + RemoteDorisProperties.FE_THRIFT_PORT;
            throw new DdlException("Required property " + property + " is missing");
        }
        if (StringUtils.isNotEmpty(addresses) && StringUtils.isNotEmpty(psm) && StringUtils.isNotEmpty(portStr)) {
            String property = RemoteDorisProperties.FE_THRIFT_HOSTS + " or " + RemoteDorisProperties.FE_PSM + ", "
                    + RemoteDorisProperties.FE_THRIFT_PORT;
            throw new DdlException("Only need one of " + property);
        }
        if ((StringUtils.isNotEmpty(psm) && StringUtils.isEmpty(portStr))
                || (StringUtils.isEmpty(psm) && StringUtils.isNotEmpty(portStr))) {
            String property = RemoteDorisProperties.FE_PSM + " " + RemoteDorisProperties.FE_THRIFT_PORT;
            throw new DdlException(property + " should both set");
        }
    }

    public List<String> getFeNodes() {
        return parseHttpHosts(catalogProperty.getOrDefault(RemoteDorisProperties.FE_HTTP_HOSTS, ""));
    }

    public List<TNetworkAddress> getFeThriftNodes() {
        String addresses = catalogProperty.getOrDefault(RemoteDorisProperties.FE_THRIFT_HOSTS, "");
        List<TNetworkAddress> tAddresses = new ArrayList<>();
        if (StringUtils.isEmpty(addresses)) {
            return tAddresses;
        }
        for (String address : addresses.split(",")) {
            int index = address.lastIndexOf(":");
            String host = address.substring(0, index);
            int port = Integer.parseInt(address.substring(index + 1));
            TNetworkAddress thriftAddress = new TNetworkAddress(host, port);
            tAddresses.add(thriftAddress);
        }
        return tAddresses;
    }

    public String getFePsm() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.FE_PSM, "");
    }

    public int getFeThriftPort() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.FE_THRIFT_PORT, "9020"));
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.PASSWORD, "");
    }

    public boolean enableSsl() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_HTTP_SSL_ENABLED,
            "false"));
    }

    public boolean isCompatible() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.COMPATIBLE,
            "false"));
    }

    public boolean enableParallelResultSink() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.ENABLE_PARALLEL_RESULT_SINK,
            "true"));
    }

    public int getQueryRetryCount() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.QUERY_RETRY_COUNT,
            "3"));
    }

    public int getQueryTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.QUERY_TIMEOUT_SEC,
            "15"));
    }

    public int getMetadataSyncRetryCount() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_SYNC_RETRIES_COUNT,
            "3"));
    }

    public int getMetadataMaxIdleConnections() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_MAX_IDLE_CONNECTIONS,
            "5"));
    }

    public int getMetadataKeepAliveDurationSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_KEEP_ALIVE_DURATION_SEC,
            "300"));
    }

    public int getMetadataConnectTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_CONNECT_TIMEOUT_SEC,
            "10"));
    }

    public int getMetadataReadTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_READ_TIMEOUT_SEC,
            "10"));
    }

    public int getMetadataWriteTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_WRITE_TIMEOUT_SEC,
            "10"));
    }

    public int getMetadataCallTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_CALL_TIMEOUT_SEC,
            "0"));
    }

    public boolean useArrowFlight() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.USE_ARROW_FLIGHT,
                "false"));
    }

    @Override
    protected void initLocalObjectsImpl() {
        client = new FeServiceClient(name, getFeThriftNodes(), getFePsm(), getFeThriftPort(),
                getUsername(), getPassword(),
                getMetadataSyncRetryCount(), getMetadataReadTimeoutSec() * 1000);
        if (!client.health()) {
            throw new RuntimeException("Failed to connect to Doris cluster,"
                    + " please check your Doris cluster or your Doris catalog configuration.");
        }
    }

    protected List<String> listDatabaseNames() {
        if (client == null) {
            makeSureInitialized();
        }
        return client.listDatabaseNames();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return client.listTableNames(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return client.tableExist(dbName, tblName);
    }

    public FeServiceClient getFeServiceClient() {
        return client;
    }

    private List<String> parseHttpHosts(String hosts) {
        String[] hostUrls = hosts.trim().split(",");
        fillUrlsWithSchema(hostUrls, enableSsl());
        return Arrays.asList(hostUrls);
    }

    private void fillUrlsWithSchema(String[] urls, boolean isSslEnabled) {
        for (int i = 0; i < urls.length; i++) {
            String seed = urls[i].trim();
            if (!seed.startsWith("http://") && !seed.startsWith("https://")) {
                urls[i] = (isSslEnabled ? "https://" : "http://") + seed;
            }
        }
    }

    private List<String> parseArrowHosts(String hosts) {
        return Arrays.asList(hosts.trim().split(","));
    }

    public static String getCatalogType() {
        return InitCatalogLog.Type.REMOTE_DORIS.name().toLowerCase(Locale.ROOT);
    }
}

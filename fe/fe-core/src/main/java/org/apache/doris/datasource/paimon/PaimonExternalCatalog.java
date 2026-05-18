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
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.service.GdprService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class PaimonExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonExternalCatalog.class);
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_FILESYSTEM = "filesystem";
    public static final String PAIMON_HMS = "hms";
    public static final String PAIMON_DLF = "dlf";
    protected String catalogType;
    protected Catalog catalog;
    protected AuthenticationConfig authConf;
    protected HadoopAuthenticator hadoopAuthenticator;

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            PaimonProperties.WAREHOUSE
    );

    public PaimonExternalCatalog(long catalogId, String name, String resource,
                                 Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.PAIMON, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        Configuration conf = DFSFileSystem.getHdfsConf(ifNotSetFallbackToSimpleAuth());
        for (Map.Entry<String, String> propEntry : this.catalogProperty.getHadoopProperties().entrySet()) {
            conf.set(propEntry.getKey(), propEntry.getValue());
        }
        authConf = AuthenticationConfig.getKerberosConfig(conf);
        hadoopAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authConf);
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

    protected List<String> listDatabaseNames() {
        try {
            return hadoopAuthenticator.doAs(() -> new ArrayList<>(catalog.listDatabases()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to list databases names, catalog name: " + getName(), e);
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return hadoopAuthenticator.doAs(() -> {
                if (tblName.contains(Catalog.SYSTEM_TABLE_SPLITTER)) {
                    try {
                        getPaimonTable(dbName, tblName);
                        return true;
                    } catch (Exception e) {
                        LOG.warn("Paimon system table does not exist, db: {}, table: {}, reason: {}",
                                dbName, tblName, e.getMessage());
                        return false;
                    }
                }
                try {
                    catalog.getTable(Identifier.create(dbName, tblName));
                    return true;
                } catch (TableNotExistException e) {
                    return false;
                }
            });

        } catch (IOException e) {
            throw new RuntimeException("Failed to check table existence, catalog name: " + getName(), e);
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        try {
            return hadoopAuthenticator.doAs(() -> {
                List<String> tableNames = null;
                try {
                    tableNames = catalog.listTables(dbName);
                } catch (Catalog.DatabaseNotExistException e) {
                    LOG.warn("DatabaseNotExistException", e);
                }
                return tableNames;
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to list table names, catalog name: " + getName(), e);
        }
    }

    public org.apache.paimon.table.Table getPaimonTable(String dbName, String tblName) {
        makeSureInitialized();
        try {
            return hadoopAuthenticator.doAs(() -> {
                if (tblName.contains(Catalog.SYSTEM_TABLE_SPLITTER)) {
                    // system table
                    String[] splits = tblName.split(String.format("\\%s", Catalog.SYSTEM_TABLE_SPLITTER));
                    if (splits.length != 2) {
                        throw new RuntimeException("Invalid system table name: " + tblName);
                    }
                    org.apache.paimon.table.Table originTable = catalog.getTable(Identifier.create(dbName, splits[0]));
                    return org.apache.paimon.table.system.SystemTableLoader.load(splits[1],
                            (org.apache.paimon.table.FileStoreTable) originTable);
                }
                return catalog.getTable(Identifier.create(dbName, tblName));
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon table:" + getName() + "."
                    + dbName + "." + tblName + ", because " + e.getMessage(), e);
        }
    }

    protected String getPaimonCatalogType(String catalogType) {
        if (PAIMON_HMS.equalsIgnoreCase(catalogType)) {
            return PaimonProperties.PAIMON_HMS_CATALOG;
        } else {
            return PaimonProperties.PAIMON_FILESYSTEM_CATALOG;
        }
    }

    protected Catalog createCatalog() {
        LOG.info("PaimonExternalCatalog[{}] createCatalog() enter", getName());
        try {
            return hadoopAuthenticator.doAs(() -> {
                long t0 = System.currentTimeMillis();
                Configuration conf = getConfiguration();
                LOG.info("PaimonExternalCatalog[{}] createCatalog.doAs, conf@{}, fs.defaultFS={}",
                        getName(), System.identityHashCode(conf), conf.get("fs.defaultFS"));

                // 尽量关掉 HDFS 的 FS 缓存，避免旧 FS 粘住旧 token
                try {
                    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
                    conf.set(DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "true");
                    LOG.info("PaimonExternalCatalog[{}] set fs.hdfs.impl.disable.cache=true", getName());
                } catch (Throwable t) {
                    LOG.warn("PaimonExternalCatalog[{}] failed to set hdfs cache configs: {}",
                            getName(), t.toString(), t);
                }

                // 1) 获取最新 token 的函数，加日志
                java.util.function.Supplier<String> tokenSupplier = () -> {
                    try {
                        String tk = GdprService.getGdprTokenFromENV();
                        LOG.debug("PaimonExternalCatalog[{}] tokenSupplier.get() -> {}",
                                getName(), tk);
                        return tk;
                    } catch (Throwable t) {
                        LOG.warn("PaimonExternalCatalog[{}] tokenSupplier exception: {}",
                                getName(), t.toString(), t);
                        return "";
                    }
                };

                // 2) 记录当前 token
                final java.util.concurrent.atomic.AtomicReference<String> lastTokenRef =
                        new java.util.concurrent.atomic.AtomicReference<>(tokenSupplier.get());

                // 3) 创建真实 Catalog 的函数，带日志
                java.util.function.Supplier<Catalog> builder = () -> {
                    long bt = System.currentTimeMillis();
                    try {
                        LOG.info("PaimonExternalCatalog[{}] builder.build() start, lastToken={}",
                                getName(), lastTokenRef.get());

                        Options options = new Options();
                        Map<String, String> paimonOptionsMap = getPaimonOptionsMap();
                        LOG.info("PaimonExternalCatalog[{}] getPaimonOptionsMap size={}",
                                getName(), paimonOptionsMap.size());
                        for (Map.Entry<String, String> kv : paimonOptionsMap.entrySet()) {
                            options.set(kv.getKey(), kv.getValue());
                        }

                        // 将当前 token 写到 conf（重点在 conf，options 只是兜底）
                        String tk = lastTokenRef.get();
                        try {
                            conf.set("ipc.client.custom_token", tk);
                            LOG.info("PaimonExternalCatalog[{}] set conf.ipc.client.custom_token={}",
                                    getName(), tk);
                        } catch (Throwable e) {
                            LOG.warn("PaimonExternalCatalog[{}] failed to set token into conf: {}",
                                    getName(), e.toString(), e);
                        }
                        // 也写一份到 options（如果你们 Paimon 那边会从 options 读取）
                        options.set("ipc.client.custom_token", tk);
                        options.set("fs.hdfs.impl.disable.cache", "true");

                        CatalogContext context = CatalogContext.create(options, conf);
                        Catalog c = createCatalogImpl(context);
                        LOG.info("PaimonExternalCatalog[{}] builder.build() success, impl={}, cost={}ms",
                                getName(), c.getClass().getName(), (System.currentTimeMillis() - bt));
                        return c;
                    } catch (Throwable t) {
                        LOG.error("PaimonExternalCatalog[{}] builder.build() failed: {}",
                                getName(), t.toString(), t);
                        throw new RuntimeException(t);
                    }
                };

                // 先建一个真实的 Catalog 实例
                final java.util.concurrent.atomic.AtomicReference<Catalog> delegate =
                        new java.util.concurrent.atomic.AtomicReference<>(builder.get());

                // 4) 判断 TOKEN_EXPIRED 的函数，加日志
                java.util.function.Predicate<Throwable> isExpired = (ex) -> {
                    Throwable t = ex;
                    while (t != null) {
                        String msg = t.getMessage();
                        if (msg != null && msg.contains("TOKEN_EXPIRED")) {
                            LOG.warn("PaimonExternalCatalog[{}] detected TOKEN_EXPIRED by message: {}",
                                    getName(), msg);
                            return true;
                        }
                        if ("org.byted.security.common.ZtiJwtException".equals(t.getClass().getName())) {
                            LOG.warn("PaimonExternalCatalog[{}] detected TOKEN_EXPIRED by class: {}",
                                    getName(), t.getClass().getName());
                            return true;
                        }
                        t = t.getCause();
                    }
                    return false;
                };

                // 5) 代理逻辑
                java.lang.reflect.InvocationHandler ih = (proxy, method, args) -> {
                    String mName = method.getName();

                    // 避免对 Object 的方法做复杂逻辑，简单转发
                    if (method.getDeclaringClass() == Object.class) {
                        if ("toString".equals(mName)) {
                            return "PaimonCatalogProxy(" + getName() + ")@" + System.identityHashCode(proxy);
                        }
                        if ("hashCode".equals(mName)) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(mName)) {
                            return proxy == args[0];
                        }
                    }

                    long start = System.currentTimeMillis();
                    LOG.debug("PaimonExternalCatalog[{}] proxy.invoke begin, method={}, argsLen={}, lastToken={}",
                            getName(), mName, (args == null ? 0 : args.length), lastTokenRef.get());
                    try {
                        // 5.1 尝试调用
                        try {
                            Object r = method.invoke(delegate.get(), args);
                            LOG.debug("PaimonExternalCatalog[{}] proxy.invoke success, method={}, cost={}ms",
                                    getName(), mName, (System.currentTimeMillis() - start));
                            return r;
                        } catch (java.lang.reflect.InvocationTargetException ite) {
                            Throwable cause = ite.getCause();
                            LOG.warn("PaimonExternalCatalog[{}] proxy.invoke got exception for method {}: {}",
                                    getName(), mName, cause.toString(), cause);
                            // 若是 TOKEN_EXPIRED，尝试刷新 token 重建一次，再重试
                            if (isExpired.test(cause)) {
                                String fresh = tokenSupplier.get();
                                lastTokenRef.set(fresh);
                                try {
                                    conf.set("ipc.client.custom_token", fresh);
                                    LOG.info("PaimonExternalCatalog[{}] TOKEN_EXPIRED -> refresh token={},"
                                            + "rebuild catalog", getName(), fresh);
                                } catch (Throwable e2) {
                                    LOG.warn("PaimonExternalCatalog[{}] failed to set fresh token into conf: {}",
                                            getName(), e2.toString(), e2);
                                }
                                delegate.set(builder.get());
                                try {
                                    Object r2 = method.invoke(delegate.get(), args);
                                    LOG.info("PaimonExternalCatalog[{}] proxy.invoke retry success,"
                                            + "method={}, cost={}ms",
                                            getName(), mName, (System.currentTimeMillis() - start));
                                    return r2;
                                } catch (java.lang.reflect.InvocationTargetException ite2) {
                                    Throwable cause2 = ite2.getCause();
                                    LOG.error("PaimonExternalCatalog[{}] proxy.invoke retry failed, method={}, ex={}",
                                            getName(), mName, cause2.toString(), cause2);
                                    if (cause2 instanceof RuntimeException) {
                                        throw (RuntimeException) cause2;
                                    }
                                    throw new RuntimeException(cause2);
                                }
                            }

                            // 非 TOKEN_EXPIRED 的异常，直接抛出去
                            if (cause instanceof RuntimeException) {
                                throw (RuntimeException) cause;
                            }
                            throw new RuntimeException(cause);
                        }
                    } finally {
                        LOG.debug("PaimonExternalCatalog[{}] proxy.invoke end, method={}, totalCost={}ms",
                                getName(), mName, (System.currentTimeMillis() - start));
                    }
                };

                Catalog proxy = (Catalog) java.lang.reflect.Proxy.newProxyInstance(
                        Catalog.class.getClassLoader(),
                        new Class<?>[] { Catalog.class },
                        ih);

                LOG.info("PaimonExternalCatalog[{}] createCatalog() success, proxy={}, totalCost={}ms",
                        getName(), proxy.getClass().getName(), (System.currentTimeMillis() - t0));
                return proxy;
            });
        } catch (IOException e) {
            LOG.error("PaimonExternalCatalog[{}] createCatalog() failed: {}", getName(), e.toString(), e);
            throw new RuntimeException("Failed to create catalog, catalog name: " + getName(), e);
        }
    }

    protected Catalog createCatalogImpl(CatalogContext context) {
        return CatalogFactory.createCatalog(context);
    }

    public Map<String, String> getPaimonOptionsMap() {
        Map<String, String> properties = catalogProperty.getHadoopProperties();
        Map<String, String> options = Maps.newHashMap();
        //options.put(PaimonProperties.WAREHOUSE, properties.get(PaimonProperties.WAREHOUSE));
        setPaimonCatalogOptions(properties, options);
        setPaimonExtraOptions(properties, options);
        return options;
    }

    protected abstract void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options);

    protected void setPaimonExtraOptions(Map<String, String> properties, Map<String, String> options) {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            if (kv.getKey().startsWith(PaimonProperties.PAIMON_PREFIX)) {
                options.put(kv.getKey().substring(PaimonProperties.PAIMON_PREFIX.length()), kv.getValue());
            }
        }

        // hive version.
        // This property is used for both FE and BE, so it has no "paimon." prefix.
        // We need to handle it separately.
        if (properties.containsKey(HMSProperties.HIVE_VERSION)) {
            options.put(HMSProperties.HIVE_VERSION, properties.get(HMSProperties.HIVE_VERSION));
        }
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }
}

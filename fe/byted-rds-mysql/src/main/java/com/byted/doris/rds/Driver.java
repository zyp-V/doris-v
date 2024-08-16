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
//
// Copied from:
// https://github.com/aliyun/datalake-catalog-metastore-client/blob/master/metastore-client-hive/metastore-client-hive2/src/main/java/com/aliyun/datalake/metastore/hive2/ProxyMetaStoreClient.java
// 3c6f5905

package com.byted.doris.rds;

import com.bytedance.mysql.MysqlDriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 *
 * A JDBC driver implementation for accessing Bytedance's RDS. We integrate the RDS
 * driver JAR into Doris so that users can use the RDS catalog out of the box without
 * any extra steps to download the driver JAR.
 *
 * Configure the following environment variables if testing the RDS catalog on your macOS:
 *
 * CONSUL_HTTP_HOST=10.225.68.72
 * CONSUL_HTTP_PORT=8000
 *
 *
 * [DDL Example]
 *
 * CREATE CATALOG mysql PROPERTIES (
 *     "type"="jdbc",
 *     "user"="",
 *     "password"="",
 *     "jdbc_url" = "jdbc:mysql:///your_db_name?db_consul=your.db.psm_read&psm=your.client.psm&useUnicode=true&characterEncoding=utf-8&auth_enable=true",
 *     "driver_url" = "byted-rds-mysql-jdbc.jar",
 *     "driver_class" = "com.byted.doris.rds.Driver"
 * );
 *
 */
public class Driver extends com.mysql.cj.jdbc.NonRegisteringDriver {

    private static final Logger LOG = LogManager.getLogger(Driver.class);
    private static final Pattern RDS_REQUIRED_PARAMS_PATTERN = Pattern.compile("(?=.*db_consul=)(?=.*psm=).*");

    public Driver() throws SQLException {
    }

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            DriverManager.registerDriver(new Driver());
            // Enable JDBC logging to help troubleshoot ZTI issues (Logs are output to the log4j console appender).
            DriverManager.setLogWriter(new PrintWriter(System.out));
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Unable to register the RDS driver! Error: " + e.getMessage(), e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            LOG.info("Connecting to RDS, url: {}", url);

            if (acceptsURL(url)) {
                return MysqlDriverManager.getConnection(url);
            }
        } catch (Throwable e) {
            LOG.warn("Connection to RDS failed, url: {}", url, e);
            throw new SQLException(e);
        }
        return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (super.acceptsURL(url)) {
            return checkRDSRequiredParams(url);
        } else {
            return false;
        }
    }

    /**
     *
     * Check required parameters for RDS.
     *
     */
    protected boolean checkRDSRequiredParams(String url) {
        return RDS_REQUIRED_PARAMS_PATTERN.matcher(url).matches();
    }
}

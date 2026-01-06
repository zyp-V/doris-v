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

package org.apache.doris.common.security.authentication;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

public class HadoopSimpleAuthenticator implements HadoopAuthenticator {
    private static final Logger LOG = LogManager.getLogger(HadoopSimpleAuthenticator.class);

    private final String proxyUser;
    private final AtomicReference<UserGroupInformation> proxyRef = new AtomicReference<>();
    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

    public HadoopSimpleAuthenticator(SimpleAuthenticationConfig config) {
        this.proxyUser = Optional.ofNullable(config.getUsername()).orElse("hadoop");
        initProxyFromLoginUser();
        startTokenRefreshLoop();
    }

    private void initProxyFromLoginUser() {
        try {
            UserGroupInformation real = UserGroupInformation.getLoginUser();
            proxyRef.set(UserGroupInformation.createProxyUser(proxyUser, real));
            LOG.info("Init proxy UGI for {} with real user {}", proxyUser, real);
        } catch (IOException e) {
            throw new RuntimeException("Init UGI failed", e);
        }
    }

    @Override
    public UserGroupInformation getUGI() {
        return proxyRef.get();
    }

    private void startTokenRefreshLoop() {
        exec.scheduleWithFixedDelay(this::refreshHadoopProxy, 0, 60 * 60, TimeUnit.SECONDS);
    }

    private void refreshHadoopProxy() {
        try {
            if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
            } else {
                UserGroupInformation.loginUserFromSubject(null);
            }

            UserGroupInformation oldProxy = proxyRef.get();
            UserGroupInformation real2 = UserGroupInformation.getLoginUser();
            UserGroupInformation newProxy = UserGroupInformation.createProxyUser(proxyUser, real2);
            proxyRef.set(newProxy);

            if (oldProxy != null) {
                FileSystem.closeAllForUGI(oldProxy);
            }
            LOG.info("Refreshed proxy UGI for {}, real={}", proxyUser, real2);
        } catch (Throwable t) {
            LOG.warn("UGI refresh failed: {}", t.toString(), t);
        }
    }

    @PreDestroy
    public void destroy() {
        exec.shutdownNow();
    }
}

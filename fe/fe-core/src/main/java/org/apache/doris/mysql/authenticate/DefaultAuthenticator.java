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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.authenticate.password.NativePassword;
import org.apache.doris.mysql.authenticate.password.NativePasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.byted.security.common.LegacyIdentity;

import java.io.IOException;
import java.util.List;

public class DefaultAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(DefaultAuthenticator.class);
    private PasswordResolver passwordResolver;

    public DefaultAuthenticator() {
        this.passwordResolver = new NativePasswordResolver();
    }

    @Override
    public AuthenticateResponse authenticate(AuthenticateRequest request) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{} start to gdpr authenticate.", request.getUserName());
        }
        if (Config.enable_gdpr && !Config.enable_gemini) {
            try {
                ImmutablePair<LegacyIdentity, String> gdprIdentityToken = Env.getCurrentEnv().getGdprService()
                        .verifyGdprAccount(request.getGdprAccountOrUserName());
                String gdprUser = gdprIdentityToken.getLeft().User.replace(".", "_");
                return new AuthenticateResponse(true, UserIdentity.createAnalyzedUserIdentWithIp(gdprUser, "%"),
                    gdprIdentityToken, gdprUser);
            } catch (AuthenticationException e) {
                MetricRepo.COUNTER_MYSQL_GDPR_AUTH_FAILED.increase(1L);
            }
        }
        if (Config.enable_gdpr && Config.enable_gemini) {
            try {
                ImmutablePair<LegacyIdentity, String> gdprIdentityToken = Env.getCurrentEnv().getGdprService()
                        .verifyGdprAccount(request.getGdprAccountOrUserName());
                String byteUserName = gdprIdentityToken.getLeft().User;
                String gdprUser = gdprIdentityToken.getLeft().User.replace(".", "_");
                return new AuthenticateResponse(true, byteUserName,
                    UserIdentity.createAnalyzedUserIdentWithIp(gdprUser, "%"),
                    gdprIdentityToken, gdprUser);
            } catch (AuthenticationException e) {
                MetricRepo.COUNTER_MYSQL_GDPR_AUTH_FAILED.increase(1L);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{}, byteUser:{} start to default authenticate.",
                    request.getDefaultUserName(), request.getByteUserName());
        }

        Password password = request.getPassword();
        if (!(password instanceof NativePassword)) {
            return AuthenticateResponse.failedResponse;
        }
        NativePassword nativePassword = (NativePassword) password;

        List<UserIdentity> currentUserIdentity = Lists.newArrayList();
        try {
            Env.getCurrentEnv().getAuth().checkPassword(request.getDefaultUserName(), request.getRemoteIp(),
                    nativePassword.getRemotePasswd(), nativePassword.getRandomString(), currentUserIdentity);
        } catch (AuthenticationException e) {
            ErrorReport.report(e.errorCode, e.msgs);
            return AuthenticateResponse.failedResponse;
        }
        return new AuthenticateResponse(true, request.getByteUserName(),
            currentUserIdentity.get(0), request.getDefaultUserName());
    }

    @Override
    public boolean canDeal(String qualifiedUser) {
        return true;
    }

    @Override
    public PasswordResolver getPasswordResolver() {
        return passwordResolver;
    }
}

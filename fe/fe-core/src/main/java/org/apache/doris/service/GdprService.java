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

package org.apache.doris.service;

import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.system.SystemInfoService;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.byted.infsec.sdk.SecToken;
import org.byted.security.common.LegacyIdentity;
import org.byted.security.common.ZtiJwtException;
import org.byted.security.token.Token;
import org.byted.security.ztijwthelper.ZTIJwtHelper;

import java.util.concurrent.TimeUnit;

public class GdprService {

    private static final Logger LOG = LogManager.getLogger(GdprService.class);

    private final Cache<String /* temp account in doris */,
            ImmutablePair<LegacyIdentity /* verified token info */, String /* gdpr token */>> accountUserCache;

    private final Privilege[] paloPrivileges = {Privilege.SELECT_PRIV,
        Privilege.ALTER_PRIV, Privilege.LOAD_PRIV, Privilege.CREATE_PRIV,
        Privilege.DROP_PRIV};

    public GdprService() {
        this.accountUserCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Config.gdpr_account_expire_time_ms, TimeUnit.MILLISECONDS)
            .maximumSize(Config.gdpr_account_cache_size)
            .build();
    }

    // TODO(weihongkai.me): get client source ip from be
    public LegacyIdentity verifyGdprToken(final String gdprToken, final String sourceIp)
            throws AuthenticationException {

        if (StringUtils.isBlank(gdprToken)) {
            if (LOG.isDebugEnabled()) {
                LOG.info("gdpr verification failed: empty token");
            }
            throw new AuthenticationException("token is empty");
        }

        String ip = sourceIp == null ? "" : sourceIp;
        String tokenFromRestfulApi = gdprToken.trim();
        try {
            return Token.verifyToken(tokenFromRestfulApi, ip, "doris");
        } catch (ZtiJwtException e) {
            if (LOG.isDebugEnabled()) {
                LOG.info("gdpr verification failed", e);
            }
            throw new AuthenticationException("Verify token error." + e.toString());
        }
    }

    // TODO(weihongkai.me): Support multi-cluster in doris, only support default_cluster now
    // resource: cluster:database.table
    private boolean checkResource(final LegacyIdentity identity, final String resource,
                                  final String operation) {
        return SecToken.CheckPermission(
            "doris", "table", resource, operation,
            identity.PSM, identity.User + "@bytedance.com");
    }

    private boolean checkResource(final LegacyIdentity identity, final String resource,
                                  final PrivPredicate operations) {
        // check privs in acp
        boolean containSupportedPriv = false;
        for (Privilege priv : paloPrivileges) {
            if (operations.getPrivs().containsPrivs(priv) && !checkResource(identity, resource,
                    priv.toString())) {
                return false;
            }

            // if operations could not contain supported priv, should return false
            if (operations.getPrivs().containsPrivs(priv)) {
                containSupportedPriv = true;
            }
        }

        return containSupportedPriv;
    }

    // serve for PaloAuth checkTblPriv
    public boolean checkResource(final LegacyIdentity identity, final String database, final String table,
            final PrivPredicate operations) throws AuthenticationException {

        checkPermissionParameter(identity, database, table, operations);
        String[] clusterDb = database.split(":");
        String db = clusterDb.length == 2 ? clusterDb[1] : clusterDb[0];
        String resource = String.format("%s:%s.%s", SystemInfoService.DEFAULT_CLUSTER, db, table);

        return checkResource(identity, resource, operations);
    }


    public boolean checkGlobalPriv(final LegacyIdentity identity) {
        if (identity == null) {
            return false;
        }
        return SecToken.CheckPermission("doris", "globalPriv", "all", "check", identity.PSM,
                identity.User + "@bytedance.com");
    }

    public boolean checkGlobalPriv(final String gdprToken, final String sourceIp) {
        if (StringUtils.isBlank(gdprToken)) {
            return false;
        }
        LegacyIdentity identity;
        try {
            identity = verifyGdprToken(gdprToken, sourceIp);
        } catch (AuthenticationException e) {
            LOG.error("Gdpr verify token failed", e);
            return false;
        }

        boolean result = checkGlobalPriv(identity);
        if (result) {
            LOG.info("Gdpr check global priv succeed, user: {}, psm: {}, ip: {}",
                    identity.User, identity.PSM,
                    sourceIp == null ? "" : sourceIp);
        }
        return result;
    }

    // serve for FrontendServiceImpl to stream load
    public void verifyTokenAndCheckPermission(final String database, final String table,
                                              final String gdprToken,
                                              final PrivPredicate operation, final String sourceIp)
            throws AuthenticationException {

        if (StringUtils.isBlank(gdprToken)) {
            throw new AuthenticationException("token is empty");
        }

        LegacyIdentity identity = verifyGdprToken(gdprToken, sourceIp);
        checkPermissionParameter(identity, database, table, operation);

        String[] clusterDb = database.split(":");
        String db = clusterDb.length == 2 ? clusterDb[1] : clusterDb[0];
        String resource = String.format("%s:%s.%s", SystemInfoService.DEFAULT_CLUSTER, db, table);
        if (!checkResource(identity, resource, operation)) {
            LOG.error("Gdpr check resource permission failed, "
                    + "user: {}, psm: {}, ip: {}, db: {}, table: {}, priv: {}",
                    identity.User, identity.PSM,
                    sourceIp == null ? "" : sourceIp,
                    db, table, operation.getPrivs().toString());
            throw new AuthenticationException(
                "Gdpr check permission failed, user: " + identity.User
                    + " has no permission to resource: "
                    + resource);
        }
    }

    public String genTempAccount(final String gdprToken, final String sourceIp)
            throws AuthenticationException {
        LegacyIdentity identity = verifyGdprToken(gdprToken, sourceIp);
        String account = RandomStringUtils.randomAlphabetic(10);
        ImmutablePair<LegacyIdentity, String> gdprTokenInfo = ImmutablePair.of(identity, gdprToken);
        accountUserCache.put(account, gdprTokenInfo);
        LOG.info("Gen gdpr account: {}, user: {}, psm: {}, ip: {}", account, identity.User,
                identity.PSM, sourceIp);
        return account;
    }

    public ImmutablePair<LegacyIdentity, String> verifyGdprAccount(final String gdprAccount)
            throws AuthenticationException {
        if (StringUtils.isBlank(gdprAccount)) {
            throw new AuthenticationException("gdprAccount is empty");
        }

        ImmutablePair<LegacyIdentity, String> gdprIdentityToken = accountUserCache.getIfPresent(gdprAccount);
        if (gdprIdentityToken == null) {
            throw new AuthenticationException("Identity is empty, gdprAccount is invalid");
        }
        return gdprIdentityToken;
    }

    private void checkPermissionParameter(final LegacyIdentity identity, final String database,
                                          final String table, final PrivPredicate operation)
            throws AuthenticationException {
        if (identity == null) {
            throw new AuthenticationException("Identity is empty");
        }

        if (StringUtils.isBlank(database) || StringUtils.isBlank(table)) {
            throw new AuthenticationException("db or table name is invalid");
        }

        if (operation == null) {
            throw new AuthenticationException("priv is null");
        }
    }

    public static String getGdprTokenFromENV() {
        String result = "";
        try {
            result = ZTIJwtHelper.getJwtSVID();
            if (LOG.isDebugEnabled()) {
                LOG.info("get Token from environment success, token={}", result);
            }

        } catch (Exception e) {
            LOG.warn("get Token from environment failed. ", e);
        }
        return result.trim();
    }
}

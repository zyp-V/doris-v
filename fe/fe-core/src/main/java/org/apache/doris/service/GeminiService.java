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
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.byted.security.common.LegacyIdentity;
import org.byted.security.common.ZtiJwtException;
import org.byted.security.token.Token;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class GeminiService {
    private static final Logger LOG = LogManager.getLogger(GeminiService.class);
    public static final String HEAD_KEY_1 = "Authorization";
    public static final String HEAD_KEY_2 = "Content-Type";
    public static final String HEAD_VALUE_2 = "application/json";
    public static final String GEMINI_API = "/api/v2/verify/verifyPrivilege";
    public static final String GEMINI_AUTH_HIVE_TYPE = "hive";
    public static final String GEMINI_AUTH_DORIS_TYPE = "doris";
    public static final String GEMINI_ALL_AUTH = "all";
    public static final String GEMINI_SELECT_AUTH = "select";
    public static final String GEMINI_CREATE_AUTH = "create";
    public static final String GEMINI_PRI_AUTH_TYPE = "user";
    public static final String BYTE_HIVE_CATALOG_NAME = "hms";
    public static final String BYTE_PAIMON_CATALOG_NAME = "paimon";
    public static final String BYTE_HIVE_ERROR_PREFIX = "ByteHive Gemini Permission denied";


    public static final int DEFAULT_TIME_OUT_MS = 2000;

    private final Cache<String /* temp account in doris */,
            ImmutablePair<LegacyIdentity /* verified token info */, String /* gdpr token */>> accountUserCache;

    private final Privilege[] paloPrivileges = {Privilege.SELECT_PRIV,
        Privilege.ALTER_PRIV, Privilege.LOAD_PRIV, Privilege.CREATE_PRIV,
        Privilege.DROP_PRIV};

    public GeminiService() {
        this.accountUserCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Config.gdpr_account_expire_time_ms, TimeUnit.MILLISECONDS)
            .maximumSize(Config.gdpr_account_cache_size)
            .build();
    }

    public boolean checkResource(String byteUserName, String db, String tbl, List<String> cols, PrivPredicate wanted,
                                 String geminiAuthHiveType)
            throws Exception {
        String authType = "";
        if (wanted.getPrivs().containsPrivs(Privilege.CREATE_PRIV)
                || wanted.getPrivs().containsPrivs(Privilege.DROP_PRIV)) {
            authType = GEMINI_CREATE_AUTH;
        }   else if (wanted.getPrivs().containsPrivs(Privilege.LOAD_PRIV)
                || wanted.getPrivs().containsPrivs(Privilege.ALTER_PRIV)) {
            authType = GEMINI_ALL_AUTH;
        } else {
            authType = GEMINI_SELECT_AUTH;
        }
        GeminiService.PriVerifyDatasource priVerifyDatasource = new GeminiService.PriVerifyDatasource();
        priVerifyDatasource.setDatasource(geminiAuthHiveType);
        priVerifyDatasource.setDatabase(db);
        priVerifyDatasource.setTable(tbl);
        priVerifyDatasource.setPriAction(authType);
        priVerifyDatasource.setColumns(cols);
        priVerifyDatasource.setRows(null);
        GeminiService.VerifyRequest verifyRequest = new GeminiService.VerifyRequest();
        verifyRequest.setUser(byteUserName);
        verifyRequest.setPsm("");
        verifyRequest.setPriAuthType(GeminiService.GEMINI_PRI_AUTH_TYPE);
        verifyRequest.setDatasources(Arrays.asList(priVerifyDatasource));
        GeminiService.VerifyResponse verifyResponse = GeminiService.verifyPrivilege(verifyRequest);
        LOG.info("Gemini request {}, response {}", new ObjectMapper().writeValueAsString(verifyRequest),
            new ObjectMapper().writeValueAsString(verifyResponse));
        if (verifyResponse.getData() == null) {
            return false;
        }
        return verifyResponse.getData().isResult();
    }

    public static String doPost(String url, Map<String, String> headers, Object body) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (Objects.nonNull(body)) {
            httpPost.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(body), "UTF-8"));
        }
        HttpUtils.setRequestConfig(httpPost, headers, DEFAULT_TIME_OUT_MS);
        return HttpUtils.executeRequest(httpPost);
    }

    public static VerifyResponse verifyPrivilege(VerifyRequest verifyRequest) throws AuthenticationException {
        VerifyResponse verifyResponse;
        try {
            String response = doPost(Config.gemini_url + GEMINI_API,
                    ImmutableMap.<String, String>builder().put(HEAD_KEY_1, Config.gemini_doris_token)
                    .put(HEAD_KEY_2, HEAD_VALUE_2).build(),
                    verifyRequest);
            verifyResponse = new ObjectMapper().readValue(response, VerifyResponse.class);
        }  catch (Exception e) {
            throw new AuthenticationException("gemini auth failed," + e.toString());
        }
        if (LOG.isDebugEnabled()) {
            LOG.info("gemini auth user {}, table {}, result {}", verifyRequest.getUser(),
                    verifyRequest.getDatasources().get(0).getTable(), verifyResponse.getData().isResult());
        }
        return verifyResponse;
    }

    public static VerifyRequest buidAuthParams(
            String geminiAuthType,
            String database,
            String table,
            String priAction,
            List<String> columns,
            List<String> rows,
            String byteUserName) {
        GeminiService.PriVerifyDatasource priVerifyDatasource = new GeminiService.PriVerifyDatasource();
        priVerifyDatasource.setDatasource(geminiAuthType);
        priVerifyDatasource.setDatabase(database);
        priVerifyDatasource.setTable(table);
        priVerifyDatasource.setPriAction(priAction);
        priVerifyDatasource.setColumns(columns);
        priVerifyDatasource.setRows(null);
        GeminiService.VerifyRequest verifyRequest = new GeminiService.VerifyRequest();
        verifyRequest.setUser(byteUserName);
        verifyRequest.setPsm("");
        verifyRequest.setPriAuthType("user");
        verifyRequest.setDatasources(Arrays.asList(priVerifyDatasource));
        return verifyRequest;
    }

    public LegacyIdentity verifyGdprToken(final String byteUserName, final String sourceIp)
            throws AuthenticationException {
        if (StringUtils.isBlank(byteUserName)) {
            if (LOG.isDebugEnabled()) {
                LOG.info("gdpr verification failed: empty byteUserName");
            }
            throw new AuthenticationException("byteUserName is empty");
        }
        String ip = sourceIp == null ? "" : sourceIp;
        String tokenFromRestfulApi = byteUserName.trim();
        try {
            return Token.verifyToken(tokenFromRestfulApi, ip, "doris");
        } catch (ZtiJwtException e) {
            if (LOG.isDebugEnabled()) {
                LOG.info("gdpr verification failed", e);
            }
            throw new AuthenticationException("Verify token error." + e.toString());
        }
    }

    public String genTempAccount(String byteUserName, final String sourceIp) throws AuthenticationException {
        LegacyIdentity identity = verifyGdprToken(byteUserName, sourceIp);
        String account = RandomStringUtils.randomAlphabetic(10);
        ImmutablePair<LegacyIdentity, String> gdprTokenInfo = ImmutablePair.of(identity, byteUserName);
        accountUserCache.put(account, gdprTokenInfo);
        LOG.info("Gen gdpr account: {}, user: {}, psm: {}, ip: {}", account, identity.User, identity.PSM, sourceIp);
        return account;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PriVerifyDatasource {
        @JsonProperty("database")
        private String database;
        @JsonProperty("priAction")
        private String priAction;
        @JsonProperty("datasource")
        private String datasource;
        @JsonProperty("columns")
        private List<String> columns;
        @JsonProperty("rows")
        private List<String> rows;
        @JsonProperty("table")
        private String table;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VerifyRequest {
        @JsonProperty("user")
        private String user;
        @JsonProperty("psm")
        private String psm;
        @JsonProperty("priAuthType")
        private String priAuthType;
        @JsonProperty("datasources")
        private List<PriVerifyDatasource> datasources;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UnAuthPri {
        private String datasource;
        private String database;
        private String table;
        private List<String> columns;
        private List<String> rows;
        private String priAction;
        private String version;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ReasonItem {
        private String priAuthType;
        private List<UnAuthPri> unAuthPri;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VerifyData {
        private boolean result;
        private String message;
        private List<ReasonItem> reason;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class VerifyResponse {
        private int code;
        private String msg;
        private VerifyData data;
        private String extra;
        private String logId;
        private String message;
    }
}

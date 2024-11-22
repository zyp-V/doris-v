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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ActionStatus;
import org.apache.doris.service.FrontendOptions;

import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GenerateAccountAction extends RestBaseController {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateAccountAction.class);

    @RequestMapping(path = "/api/account/gen", method = RequestMethod.POST)
    public Object gen(HttpServletRequest request, HttpServletResponse response) {
        LOG.info("received generate GDPR account request");
        response.setContentType("application/json");
        if (!Config.enable_gdpr) {
            LOG.warn("failed to generate gdpr account, gdpr not enabled");
            return responseFormat(ActionStatus.GDPR_NOT_ENABLE.ordinal(),
                    "Gdpr function is disabled", "");
        }

        String gdprToken = request.getParameter("token");
        if (gdprToken == null) {
            LOG.warn("failed to generate gdpr account, token is empty");
            return responseFormat(HttpResponseStatus.BAD_REQUEST.code(),
                    "Parameter token is empty", "");
        }

        String account;
        try {
            account = Env.getCurrentEnv().getGdprService().genTempAccount(
                gdprToken, request.getRemoteHost());
        } catch (Exception e) {
            LOG.error("failed to generate gdpr account", e);
            return responseFormat(ActionStatus.INVALID_GDPR_TOKEN.ordinal(), "verify token error: " + e, "");
        }

        return responseFormat(ActionStatus.OK.ordinal(), "", account);
    }

    private String responseFormat(int errno, String errmsg, String username) {
        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("errno", String.valueOf(errno));
        jsonObject.put("errmsg", errmsg);
        jsonObject.put("username", username);
        jsonObject.put("fe_port", String.valueOf(Config.query_port));
        if (FrontendOptions.isBindIPV6()) {
            jsonObject.put("fe_host_ipv6", Env.getCurrentEnv().getSelfNode().getHost());
        } else {
            jsonObject.put("fe_host", Env.getCurrentEnv().getSelfNode().getHost());
        }
        return new Gson().toJson(jsonObject);
    }
}

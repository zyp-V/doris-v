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

package org.apache.doris.httpv2.interceptor;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import org.byted.security.common.LegacyIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GDPRInterceptor extends BaseController implements HandlerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(GDPRInterceptor.class);

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) throws Exception {
        verifyGdpr(request);
        return true;
    }

    private void verifyGdpr(HttpServletRequest request) throws AuthenticationException {
        ConnectContext ctx = new ConnectContext(null);
        if (Config.enable_gdpr) {
            LegacyIdentity identity = null;
            try {
                identity = Env.getCurrentEnv().getGdprService().verifyGdprToken(
                    request.getParameter("token"),
                    request.getRemoteHost());
            } catch (AuthenticationException e) {
                MetricRepo.COUNTER_STREAM_LOAD_GDPR_AUTH_FAILED.increase(1L);
            }
            /*
                verify gdpr token succeed, build ConnectContext to avoid NullPointerException
                if basic auth failed
            */
            if (identity != null) {
                ctx.setGdprIdentity(identity);
                String user = identity.User.replace(".", "_");
                buildConnectContext(Env.getCurrentEnv(), user,
                        request.getRemoteHost(),
                        UserIdentity.createAnalyzedUserIdentWithIp(user, "%"),
                        SystemInfoService.DEFAULT_CLUSTER, ctx);
            }
        }
        ctx.setThreadLocalInfo();
    }

    private void buildConnectContext(Env env, String qualifiedUser, String remoteIP,
                                     UserIdentity currentUser, String cluster, ConnectContext ctx) {
        ctx.setEnv(env);
        ctx.setQualifiedUser(qualifiedUser);
        ctx.setRemoteIP(remoteIP);
        ctx.setCurrentUserIdentity(currentUser);
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response,
                           Object handler, ModelAndView modelAndView) throws Exception {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler, Exception ex) throws Exception {
    }
}

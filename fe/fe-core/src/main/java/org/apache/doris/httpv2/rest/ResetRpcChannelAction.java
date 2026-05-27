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

import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// This class is a RESTFUL interface to remove specific BE rpc cache on FE
// It will be used in query monitor to collect profiles.
@RestController
public class ResetRpcChannelAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ResetRpcChannelAction.class);

    @RequestMapping(path = "/api/reset_rpc_channel", method = RequestMethod.GET)
    protected Object reset_rpc_channel(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        String endpoints = request.getParameter("endpoints");
        if (Strings.isNullOrEmpty(endpoints)) {
            return ResponseEntityBuilder.badRequest("Missing endpoints input");
        }
        for (String endpoint : endpoints.split(",")) {
            String host = "";
            try {
                host = endpoint.substring(0, endpoint.lastIndexOf(":"));
                InetAddress address = InetAddress.getByName(host);
                if (address == null) {
                    return ResponseEntityBuilder.badRequest("Error while parsing endpoint=" + endpoint
                            + ", Illegal host format: " + host);
                }
            } catch (UnknownHostException e) {
                return ResponseEntityBuilder.badRequest("Error while parsing endpoint=" + endpoint
                        + ", Illegal host format, : " + host + ", msg = " + e.getMessage());
            }
            int port = 0;
            try {
                port = Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(":") + 1));
            } catch (Exception e) {
                return ResponseEntityBuilder.badRequest("Error while parsing endpoint=" + endpoint
                        + ", Illegal port format=" + port + ", msg = " + e.getMessage());
            }
            LOG.info("removing BackendServiceProxy [{}:{}] ", host, port);
            BackendServiceProxy.removeProxyAll(new TNetworkAddress(host, port));
            LOG.info("BackendServiceProxy [{}:{}] has been removed", host, port);
        }

        return ResponseEntityBuilder.ok();
    }
}

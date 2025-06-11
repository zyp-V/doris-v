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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.mysql.authenticate.password.Password;

public class AuthenticateRequest {
    // qualifiedUser consists of ${DEFAULT_USER}:${GDPR_USER} or just a single ${USER}
    private String userName;
    private Password password;
    private String remoteIp;

    public AuthenticateRequest(String userName, Password password, String remoteIp) {
        this.userName = userName;
        this.password = password;
        this.remoteIp = remoteIp;
    }

    public String getUserName() {
        return userName;
    }

    public String getGdprAccountOrUserName() {
        String[] splits = this.userName.split(ClusterNamespace.CLUSTER_DELIMITER);
        return splits[0];
    }

    public String getByteUserName() {
        String[] splits = this.userName.split(ClusterNamespace.CLUSTER_DELIMITER);
        String byteUserName = "";
        if (splits.length == 2) {
            byteUserName = splits[0];
        } else if (splits.length == 3) {
            byteUserName = splits[0];
        } else if (splits.length < 2 || splits.length > 3) {
            byteUserName = "";
        }
        return byteUserName;
    }

    public String getDefaultUserName() {
        String[] splits = this.userName.split(ClusterNamespace.CLUSTER_DELIMITER);
        String gdprAccountOrUserName = splits.length == 1 ? splits[0] : splits[1];
        return gdprAccountOrUserName;
    }

    public Password getPassword() {
        return password;
    }

    public String getRemoteIp() {
        return remoteIp;
    }
}

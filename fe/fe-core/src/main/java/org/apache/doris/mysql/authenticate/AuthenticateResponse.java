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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.byted.security.common.LegacyIdentity;

public class AuthenticateResponse {
    public static AuthenticateResponse failedResponse = new AuthenticateResponse(false, null);

    private boolean success;
    private UserIdentity userIdentity;
    private boolean isTemp = false;
    private String byteUserName;
    private ImmutablePair<LegacyIdentity, String> gdprIdentityToken = null;
    private String qualifiedUser;

    // for test only
    public AuthenticateResponse(boolean success) {
        this.success = success;
    }

    public AuthenticateResponse(boolean success, String qualifiedUser) {
        this.success = success;
        this.qualifiedUser = qualifiedUser;
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity, String qualifiedUser) {
        this.success = success;
        this.userIdentity = userIdentity;
        this.qualifiedUser = qualifiedUser;
    }

    public AuthenticateResponse(boolean success, String byteUserName, UserIdentity userIdentity, String qualifiedUser) {
        this.success = success;
        this.byteUserName = byteUserName;
        this.userIdentity = userIdentity;
        this.qualifiedUser = qualifiedUser;
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity,
                                ImmutablePair<LegacyIdentity, String> gdprIdentityToken, String qualifiedUser) {
        this.success = success;
        this.userIdentity = userIdentity;
        this.gdprIdentityToken = gdprIdentityToken;
        this.isTemp = true;
        this.qualifiedUser = qualifiedUser;
    }

    public AuthenticateResponse(boolean success, String byteUserName, UserIdentity userIdentity,
                                ImmutablePair<LegacyIdentity, String> gdprIdentityToken, String qualifiedUser) {
        this.success = success;
        this.byteUserName = byteUserName;
        this.userIdentity = userIdentity;
        this.gdprIdentityToken = gdprIdentityToken;
        this.isTemp = true;
        this.qualifiedUser = qualifiedUser;
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity, boolean isTemp) {
        this.success = success;
        this.userIdentity = userIdentity;
        this.isTemp = isTemp;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public void setTemp(boolean temp) {
        isTemp = temp;
    }

    public LegacyIdentity getGdprIdentity() {
        return this.gdprIdentityToken == null ?  null : this.gdprIdentityToken.getLeft();
    }

    public String getByteUserName() {
        return this.byteUserName == null ?  null : this.byteUserName;
    }

    public String getGdprToken() {
        return this.gdprIdentityToken == null ?  null : this.gdprIdentityToken.getRight();
    }

    public String getQualifiedUser() {
        return this.qualifiedUser;
    }

    @Override
    public String toString() {
        return "AuthenticateResponse{"
                + "success=" + success
                + ", userIdentity=" + userIdentity
                + ", isTemp=" + isTemp
                + ", qualifiedUser=" + qualifiedUser
                + '}';
    }
}

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

package org.apache.doris.httpv2.entity;

// Status of RESTful action
public enum ActionStatus {
    OK(0),
    FAILED(1),
    INVALID_GDPR_TOKEN(2),
    GDPR_NOT_ENABLE(3),
    ANALYZE_ERROR(5),
    NOT_SUPPORT_ERROR(6),
    BAD_REQUEST(403),
    NOT_FOUND(404),
    RESOURCE_NOT_FOUND(1001);
    public int status;
    ActionStatus(int code) {
        this.status = code;
    }

    public int getStatus() {
        return status;
    }
}

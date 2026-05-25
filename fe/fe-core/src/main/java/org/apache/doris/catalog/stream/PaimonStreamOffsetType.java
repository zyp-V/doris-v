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

package org.apache.doris.catalog.stream;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

public enum PaimonStreamOffsetType {
    SNAPSHOT_ID,
    TIMESTAMP;

    public static PaimonStreamOffsetType fromString(String type) throws AnalysisException {
        if (Strings.isNullOrEmpty(type) || "snapshot_id".equalsIgnoreCase(type)) {
            return SNAPSHOT_ID;
        }
        if ("timestamp".equalsIgnoreCase(type)) {
            return TIMESTAMP;
        }
        throw new AnalysisException("Unsupported ALTER STREAM offset type: " + type);
    }
}

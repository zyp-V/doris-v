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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class GeminiServiceTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDown() {
    }

    @Test
    public void testCreatePartitionRange() throws AuthenticationException {
        GeminiService.PriVerifyDatasource priVerifyDatasource = new GeminiService.PriVerifyDatasource();
        priVerifyDatasource.setDatasource("hive");
        priVerifyDatasource.setDatabase("olap");
        priVerifyDatasource.setTable("presto_bills_2");
        priVerifyDatasource.setPriAction(GeminiService.GEMINI_SELECT_AUTH);
        priVerifyDatasource.setColumns(Arrays.asList("cnt"));
        priVerifyDatasource.setRows(null);

        GeminiService.VerifyRequest verifyRequest = new GeminiService.VerifyRequest();
        verifyRequest.setUser("lvliangliang");
        verifyRequest.setPsm("");
        verifyRequest.setPriAuthType(GeminiService.GEMINI_PRI_AUTH_TYPE);
        verifyRequest.setDatasources(Arrays.asList(priVerifyDatasource));
        GeminiService.VerifyResponse verifyResponse = GeminiService.verifyPrivilege(verifyRequest);
        Assert.assertTrue(verifyResponse.getData().isResult());
    }
}

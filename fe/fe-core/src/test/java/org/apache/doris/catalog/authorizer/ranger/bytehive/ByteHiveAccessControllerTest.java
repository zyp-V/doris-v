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

package org.apache.doris.catalog.authorizer.ranger.bytehive;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.GeminiService;

import org.byted.security.common.LegacyIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class ByteHiveAccessControllerTest {

    @Test
    public void testCheckColsPrivDenyWhenByteUserMissing() {
        boolean oldEnableGemini = Config.enable_gemini;
        try {
            Config.enable_gemini = true;
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.getSessionVariable().enableGemini = true;
            ByteHiveAccessController controller = new ByteHiveAccessController(Mockito.mock(Auth.class));
            UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            Assertions.assertThrows(AuthorizationException.class,
                    () -> controller.checkColsPriv(user, "paimon_1", "db", "tbl",
                            Collections.singleton("c1"), PrivPredicate.SELECT));
        } finally {
            Config.enable_gemini = oldEnableGemini;
            ConnectContext.remove();
        }
    }

    @Test
    public void testCheckColsPrivAllowWhenGeminiAllow() throws Exception {
        boolean oldEnableGemini = Config.enable_gemini;
        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        try {
            Config.enable_gemini = true;
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.getSessionVariable().enableGemini = true;

            Env env = Mockito.mock(Env.class);
            GeminiService geminiService = Mockito.mock(GeminiService.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getGeminiService()).thenReturn(geminiService);
            Mockito.when(geminiService.checkResource(Mockito.eq("byte"), Mockito.eq("db"), Mockito.eq("tbl"),
                            Mockito.anyList(), Mockito.eq(PrivPredicate.SELECT), Mockito.anyString()))
                    .thenReturn(true);

            ByteHiveAccessController controller = new ByteHiveAccessController(Mockito.mock(Auth.class));
            UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            user.setByteUserName("byte");
            controller.checkColsPriv(user, "paimon_1", "db", "tbl",
                    Collections.singleton("c1"), PrivPredicate.SELECT);
        } finally {
            envStatic.close();
            Config.enable_gemini = oldEnableGemini;
            ConnectContext.remove();
        }
    }

    @Test
    public void testCheckColsPrivAllowWhenGeminiAllowWithIdentityFallback() throws Exception {
        boolean oldEnableGemini = Config.enable_gemini;
        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        try {
            Config.enable_gemini = true;
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.getSessionVariable().enableGemini = true;
            LegacyIdentity identity = new LegacyIdentity();
            identity.User = "byte";
            ctx.setGdprIdentity(identity);

            Env env = Mockito.mock(Env.class);
            GeminiService geminiService = Mockito.mock(GeminiService.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getGeminiService()).thenReturn(geminiService);
            Mockito.when(geminiService.checkResource(Mockito.eq("byte"), Mockito.eq("db"), Mockito.eq("tbl"),
                            Mockito.anyList(), Mockito.eq(PrivPredicate.SELECT), Mockito.anyString()))
                    .thenReturn(true);

            ByteHiveAccessController controller = new ByteHiveAccessController(Mockito.mock(Auth.class));
            UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            controller.checkColsPriv(user, "paimon_1", "db", "tbl",
                    Collections.singleton("c1"), PrivPredicate.SELECT);
        } finally {
            envStatic.close();
            Config.enable_gemini = oldEnableGemini;
            ConnectContext.remove();
        }
    }

    @Test
    public void testCheckColsPrivDenyWhenGeminiThrows() throws Exception {
        boolean oldEnableGemini = Config.enable_gemini;
        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        try {
            Config.enable_gemini = true;
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.getSessionVariable().enableGemini = true;

            Env env = Mockito.mock(Env.class);
            GeminiService geminiService = Mockito.mock(GeminiService.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getGeminiService()).thenReturn(geminiService);
            Mockito.when(geminiService.checkResource(Mockito.eq("byte"), Mockito.eq("db"), Mockito.eq("tbl"),
                            Mockito.anyList(), Mockito.eq(PrivPredicate.SELECT), Mockito.anyString()))
                    .thenThrow(new RuntimeException("boom"));

            ByteHiveAccessController controller = new ByteHiveAccessController(Mockito.mock(Auth.class));
            UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            user.setByteUserName("byte");
            Assertions.assertThrows(AuthorizationException.class,
                    () -> controller.checkColsPriv(user, "paimon_1", "db", "tbl",
                            Collections.singleton("c1"), PrivPredicate.SELECT));
        } finally {
            envStatic.close();
            Config.enable_gemini = oldEnableGemini;
            ConnectContext.remove();
        }
    }
}

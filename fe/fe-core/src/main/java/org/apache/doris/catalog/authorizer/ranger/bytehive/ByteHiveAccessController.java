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
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.service.GeminiService;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ByteHiveAccessController implements CatalogAccessController {
    private static final Logger LOG = LogManager.getLogger(ByteHiveAccessController.class);

    private Auth auth;

    public ByteHiveAccessController(Auth auth) {
        this.auth = auth;
    }

    @Override
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return false;
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return auth.checkCtlPriv(currentUser, ctl, wanted);
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        return auth.checkDbPriv(currentUser, ctl, db, wanted);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        return auth.checkTblPriv(currentUser, ctl, db, tbl, wanted);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
                              PrivPredicate wanted) throws AuthorizationException {
        String byteUserName = currentUser.getByteUserName();
        boolean checkResource = true;
        ConnectContext connectContext = ConnectContext.get();
        if (StringUtils.isBlank(byteUserName) && connectContext != null
                && connectContext.getGdprIdentity() != null
                && StringUtils.isNotBlank(connectContext.getGdprIdentity().User)) {
            byteUserName = connectContext.getGdprIdentity().User;
        }
        boolean enableGemini = Config.enable_gemini;
        if (enableGemini && connectContext != null) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            if (sessionVariable != null) {
                enableGemini = sessionVariable.isEnableGemini();
            }
        }
        if (enableGemini && wanted.getPrivs().containsPrivs(
                Privilege.SELECT_PRIV,
                Privilege.LOAD_PRIV,
                Privilege.ALTER_PRIV,
                Privilege.CREATE_PRIV,
                Privilege.DROP_PRIV)) {
            if (StringUtils.isBlank(byteUserName)) {
                checkResource = false;
            } else {
                try {
                    if (StringUtils.isNotBlank(db)) {
                        String[] clusterDb = db.split(":");
                        db = clusterDb.length == 2 ? clusterDb[1] : clusterDb[0];
                    }
                    checkResource = Env.getCurrentEnv().getGeminiService()
                            .checkResource(byteUserName, db, tbl, new ArrayList<>(cols),
                                    wanted, GeminiService.GEMINI_AUTH_HIVE_TYPE);
                } catch (Exception e) {
                    LOG.warn("Gemini check resource permission failed,", e);
                    checkResource = false;
                }
            }
        }
        if (!checkResource) {
            try {
                if (ConnectContext.get() != null) {
                    ConnectContext.get().getSessionVariable().enableFallbackToOriginalPlanner = false;
                }
            } catch (Exception e) {
                // ignore this.
            }
            throw new AuthorizationException(String.format(GeminiService.BYTE_HIVE_ERROR_PREFIX
                + ": user [%s] does not have column privilege for [%s] command on [%s].[%s].[%s]",
                byteUserName, wanted, ctl, db, tbl));
        }
    }

    @Override
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return auth.checkResourcePriv(currentUser, resourceName, wanted);
    }

    @Override
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return auth.checkWorkloadGroupPriv(currentUser, workloadGroupName, wanted);
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
                                                       String col) {
        return Optional.empty();
    }

    @Override
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
                                                                 String tbl) {
        return Env.getCurrentEnv().getPolicyMgr().getUserPolicies(ctl, db, tbl, currentUser);
    }
}

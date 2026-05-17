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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.doris.FeServiceClient;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TAbortRemoteTxnRequest;
import org.apache.doris.thrift.TAbortRemoteTxnResult;
import org.apache.doris.thrift.TBeginRemoteTxnRequest;
import org.apache.doris.thrift.TBeginRemoteTxnResult;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.thrift.TCommitRemoteTxnResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Remote executor for Doris Catalog remote insert.
 * Local insert implementation remains in {@link OlapInsertExecutor}.
 *
 * This executor is responsible for wiring Doris Catalog remote transaction
 * lifecycle (begin / commit / abort) while reusing the generic insert execution pipeline
 * defined in {@link AbstractInsertExecutor}.
 */
public class RemoteOlapInsertExecutor extends OlapInsertExecutor {

    private static final Logger LOG = LogManager.getLogger(RemoteOlapInsertExecutor.class);

    public RemoteOlapInsertExecutor(ConnectContext ctx, RemoteOlapTable table,
            String labelName, org.apache.doris.nereids.NereidsPlanner planner,
            java.util.Optional<InsertCommandContext> insertCtx, boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    @Override
    public void beginTransaction() {
        RemoteDorisExternalCatalog remoteCatalog = ((RemoteOlapTable) table).getCatalog();
        FeServiceClient client = remoteCatalog.getFeServiceClient();
        String remoteDbName = database.getFullName();
        String remoteTableName = table.getName();

        TBeginRemoteTxnRequest request = new TBeginRemoteTxnRequest();
        request.setCatalog(database.getCatalog().getName());
        request.setDb(remoteDbName);
        request.setTbl(remoteTableName);
        request.setLabel(labelName);
        long timeoutSeconds = ctx.getExecTimeout();
        if (timeoutSeconds > 0) {
            request.setTimeoutMs(timeoutSeconds * 1000L);
        }

        try {
            TBeginRemoteTxnResult result = client.beginRemoteTxn(request);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                switch (result.getStatus().getStatusCode()) {
                    case NOT_AUTHORIZED:
                        throw new AuthenticationException(result.getStatus().getErrorMsgs().get(0));
                    case LABEL_ALREADY_EXISTS:
                        throw new LabelAlreadyUsedException(result.getStatus().getErrorMsgs().get(0));
                    case TOO_MANY_TASKS:
                        throw new BeginTransactionException(result.getStatus().getErrorMsgs().get(0));
                    case NOT_FOUND:
                        throw new MetaNotFoundException(result.getStatus().getErrorMsgs().get(0));
                    case ANALYSIS_ERROR:
                    case INTERNAL_ERROR:
                    default:
                        throw new AnalysisException(result.getStatus().getErrorMsgs().get(0));
                }
            }
            this.txnId = result.getTxnId();
            LOG.info("begin remote txn success, catalog={}, db={}, table={}, label={}, txnId={}",
                    database.getCatalog().getName(), remoteDbName, remoteTableName, labelName, txnId);
        } catch (Exception e) {
            LOG.warn("begin remote txn failed, catalog={}, db={}, table={}, label={}, errMsg={}",
                    database.getCatalog().getName(), remoteDbName, remoteTableName, labelName, e.getMessage());
            throw new AnalysisException(Util.getRootCauseMessage(e), e);
        }
    }

    @Override
    protected void onComplete() throws UserException {
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            try {
                abortTransactionOnFail();
            } catch (Exception abortTxnException) {
                LOG.warn("errors when abort txn. {}", ctx.getQueryIdentifier(), abortTxnException);
            }
            return;
        }

        RemoteDorisExternalCatalog remoteCatalog = ((RemoteOlapTable) table).getCatalog();
        FeServiceClient client = remoteCatalog.getFeServiceClient();

        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest();
        request.setTxnId(txnId);
        request.setCatalog(database.getCatalog().getName());
        request.setDb(database.getFullName());
        request.setTbl(table.getName());
        request.setCommitInfos(coordinator.getCommitInfos());
        request.setInsertVisibleTimeoutMs(ctx.getSessionVariable().getInsertVisibleTimeoutMs());
        try {
            TCommitRemoteTxnResult result = client.commitRemoteTxn(request);
            if (result.getStatus().getStatusCode() == TStatusCode.OK) {
                if (result.isTxnStatus()) {
                    txnStatus = TransactionStatus.VISIBLE;
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }
                LOG.info("commit remote txn success, catalog={}, dbId={}, txnId={}, status={}",
                                    remoteCatalog.getName(), database.getId(), txnId, txnStatus);
            } else {
                switch (result.getStatus().getStatusCode()) {
                    case NOT_AUTHORIZED:
                        throw new AuthenticationException(result.getStatus().getErrorMsgs().get(0));
                    default:
                        throw new UserException(result.getStatus().getErrorMsgs().get(0));
                }
            }
        } catch (UserException e) {
            LOG.warn("commit remote txn failed, catalog={}, dbId={}, txnId={}, status={}, err={}",
                                    remoteCatalog.getName(), database.getId(), txnId, txnStatus, e.getMessage());
            throw e;
        } catch (Exception e) {
            throw new UserException(Util.getRootCauseMessage(e), e);
        }
    }

    /**
     * Abort remote transaction when insert into remote Doris table failed.
     * This method is best-effort and will not throw exception to user.
     */
    @Override
    protected void abortTransactionOnFail() throws Exception {
        RemoteDorisExternalCatalog remoteCatalog = ((RemoteOlapTable) table).getCatalog();
        FeServiceClient client = remoteCatalog.getFeServiceClient();
        TAbortRemoteTxnRequest request = new TAbortRemoteTxnRequest();
        request.setTxnId(txnId);
        request.setCatalog(database.getCatalog().getName());
        request.setDb(database.getFullName());
        try {
            TAbortRemoteTxnResult result = client.abortRemoteTxn(request);
            if (result.getStatus().getStatusCode() == TStatusCode.OK) {
                LOG.info("abort remote txn success, catalog={}, txnId={} ",
                        remoteCatalog.getName(), txnId);
            } else {
                LOG.warn("abort remote transaction failed. catalog={}, txnId={}, err={}",
                        remoteCatalog.getName(), txnId, result.getStatus().getErrorMsgs().get(0));
                throw new UserException(result.getStatus().getErrorMsgs().get(0));
            }
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("abort remote transaction failed unexpectedly. catalog={}, txnId={}, err={}",
                    remoteCatalog.getName(), txnId, e.getMessage(), e);
            throw new Exception(Util.getRootCauseMessage(e), e);
        }
    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.warn("insert [{}] with query id {} failed", labelName, queryId, t);
        if (txnId != INVALID_TXN_ID) {
            try {
                abortTransactionOnFail();
            } catch (Exception abortTxnException) {
                LOG.warn("insert [{}] with query id {} abort txn {} failed",
                        labelName, queryId, txnId, abortTxnException);
            }
        }
        StringBuilder sb = new StringBuilder(errMsg);
        if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
            sb.append(". url: ").append(coordinator.getTrackingUrl());
            errMsg = sb.toString();
        }
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
    }

    @Override
    protected void afterExec(StmtExecutor executor) {
        // Go here, which means:
        // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
        // 2. transaction failed but Config.using_old_load_usage_pattern is true.
        // we will record the load job info for these 2 cases
        try {
            // Do not register job if job id is -1.
            if (!Config.enable_nereids_load && jobId != -1) {
                ((RemoteOlapTable) table).getCatalog().getFeServiceClient().recordFinishedLoadJob(
                        labelName, txnId, database.getCatalog().getName(), database.getFullName(), table.getName(),
                        createTime, errMsg, coordinator.getTrackingUrl());
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
            errMsg = "Record info of insert load with error " + e.getMessage();
        }
        // set return info
        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(labelName).append("', 'status':'")
                .append(ctx.isTxnModel() ? TransactionStatus.PREPARE.name() : txnStatus.name());
        sb.append("', 'txnId':'").append(txnId).append("'");
        if (table.getType() == TableIf.TableType.MATERIALIZED_VIEW) {
            sb.append("', 'rows':'").append(loadedRows).append("'");
        }
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");

        ctx.getState().setOk(loadedRows, filteredRows, sb.toString());
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(txnId, labelName, database.getFullName(), table.getName(),
                txnStatus, loadedRows, filteredRows);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) loadedRows);
    }
}

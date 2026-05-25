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

import org.apache.doris.common.UserException;
import org.apache.doris.transaction.TransactionCommitFailedException;

import com.google.gson.annotations.SerializedName;

public class PaimonTableStreamUpdate extends AbstractTableStreamUpdate {
    @SerializedName("psi")
    private long prevSnapshotId;

    @SerializedName("nsi")
    private long nextSnapshotId;

    @SerializedName("nctm")
    private long nextCommitTimestampMs;

    public PaimonTableStreamUpdate() {
        this(0L, 0L, 0L);
    }

    public PaimonTableStreamUpdate(long prevSnapshotId, long nextSnapshotId, long nextCommitTimestampMs) {
        this.prevSnapshotId = prevSnapshotId;
        this.nextSnapshotId = nextSnapshotId;
        this.nextCommitTimestampMs = nextCommitTimestampMs;
    }

    public long getPrevSnapshotId() {
        return prevSnapshotId;
    }

    public long getNextSnapshotId() {
        return nextSnapshotId;
    }

    public long getNextCommitTimestampMs() {
        return nextCommitTimestampMs;
    }

    @Override
    public void merge(AbstractTableStreamUpdate other) throws UserException {
        if (!(other instanceof PaimonTableStreamUpdate)) {
            throw new TransactionCommitFailedException(
                    "invalid Paimon table stream update type: "
                            + (other == null ? "null" : other.getClass().getName()));
        }
        PaimonTableStreamUpdate that = (PaimonTableStreamUpdate) other;
        if (isEmpty()) {
            this.prevSnapshotId = that.prevSnapshotId;
            this.nextSnapshotId = that.nextSnapshotId;
            this.nextCommitTimestampMs = that.nextCommitTimestampMs;
            return;
        }
        if (prevSnapshotId != that.prevSnapshotId
                || nextSnapshotId != that.nextSnapshotId
                || nextCommitTimestampMs != that.nextCommitTimestampMs) {
            throw new TransactionCommitFailedException(
                    "conflicting Paimon table stream update: " + this + " vs " + that);
        }
    }

    public void checkSnapshotOffset(String dbName, String streamName, long snapshotId, long historicalSnapshotId)
            throws UserException {
        if (historicalSnapshotId > 0) {
            if (prevSnapshotId != snapshotId || nextSnapshotId < historicalSnapshotId) {
                throw new TransactionCommitFailedException("history offset already consumed: "
                        + dbName + '-' + streamName + '-' + nextSnapshotId + ", prev " + prevSnapshotId
                        + " vs " + snapshotId + ", historical " + historicalSnapshotId);
            }
            return;
        }
        if (prevSnapshotId != snapshotId) {
            throw new TransactionCommitFailedException("target offset already consumed: "
                    + dbName + '-' + streamName + '-' + nextSnapshotId + ", prev " + prevSnapshotId
                    + " vs " + snapshotId);
        }
    }

    private boolean isEmpty() {
        return prevSnapshotId == 0L && nextSnapshotId == 0L && nextCommitTimestampMs == 0L;
    }

    @Override
    public String toString() {
        return "PaimonTableStreamUpdate{"
                + "prevSnapshotId=" + prevSnapshotId
                + ", nextSnapshotId=" + nextSnapshotId
                + ", nextCommitTimestampMs=" + nextCommitTimestampMs
                + '}';
    }
}

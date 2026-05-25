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

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterStreamOffsetOperationLog implements Writable {
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("streamId")
    private long streamId;
    @SerializedName("streamName")
    private String streamName;
    @SerializedName("snapshotId")
    private long snapshotId;
    @SerializedName("commitTimestampMs")
    private long commitTimestampMs;
    @SerializedName("historicalSnapshotId")
    private long historicalSnapshotId;

    public AlterStreamOffsetOperationLog() {
    }

    public AlterStreamOffsetOperationLog(long dbId, long streamId, String streamName, long snapshotId,
            long commitTimestampMs, long historicalSnapshotId) {
        this.dbId = dbId;
        this.streamId = streamId;
        this.streamName = streamName;
        this.snapshotId = snapshotId;
        this.commitTimestampMs = commitTimestampMs;
        this.historicalSnapshotId = historicalSnapshotId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getStreamId() {
        return streamId;
    }

    public String getStreamName() {
        return streamName;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getCommitTimestampMs() {
        return commitTimestampMs;
    }

    public long getHistoricalSnapshotId() {
        return historicalSnapshotId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterStreamOffsetOperationLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterStreamOffsetOperationLog.class);
    }
}

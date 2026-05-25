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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.stream.PaimonStreamOffsetType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

import java.util.Map;
import java.util.Map.Entry;

/** Command for ALTER STREAM SET PROPERTIES. */
public class AlterStreamSetPropertiesCommand extends Command implements ForwardWithSync {
    public static final String OFFSET = "offset";
    public static final String STREAM_OFFSET = "stream.offset";
    public static final String TYPE = "type";

    private final TableNameInfo streamName;
    private final boolean ifExists;
    private final Map<String, String> properties;

    public AlterStreamSetPropertiesCommand(boolean ifExists, TableNameInfo streamName,
            Map<String, String> properties) {
        super(PlanType.ALTER_STREAM_SET_PROPERTIES_COMMAND);
        this.ifExists = ifExists;
        this.streamName = streamName;
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        streamName.analyze(ctx);
        AlterStreamOffsetProperties offsetProperties = analyzeProperties();
        Env.getCurrentEnv().alterTableStream(streamName, ifExists, offsetProperties.offsetValue,
                offsetProperties.offsetType);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    private AlterStreamOffsetProperties analyzeProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("ALTER STREAM SET requires properties.");
        }
        String offsetValue = getPropertyIgnoreCase(OFFSET);
        offsetValue = Strings.isNullOrEmpty(offsetValue) ? getPropertyIgnoreCase(STREAM_OFFSET) : offsetValue;
        if (Strings.isNullOrEmpty(offsetValue)) {
            throw new AnalysisException("ALTER STREAM SET only supports offset property.");
        }
        PaimonStreamOffsetType offsetType = PaimonStreamOffsetType.fromString(getPropertyIgnoreCase(TYPE));
        for (String key : properties.keySet()) {
            if (!OFFSET.equalsIgnoreCase(key) && !STREAM_OFFSET.equalsIgnoreCase(key)
                    && !TYPE.equalsIgnoreCase(key)) {
                throw new AnalysisException("Unsupported ALTER STREAM property: " + key);
            }
        }
        try {
            Long.parseLong(offsetValue);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid ALTER STREAM offset: " + offsetValue, e);
        }
        return new AlterStreamOffsetProperties(offsetValue, offsetType);
    }

    private String getPropertyIgnoreCase(String propertyKey) {
        for (Entry<String, String> entry : properties.entrySet()) {
            if (propertyKey.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static class AlterStreamOffsetProperties {
        private final String offsetValue;
        private final PaimonStreamOffsetType offsetType;

        private AlterStreamOffsetProperties(String offsetValue, PaimonStreamOffsetType offsetType) {
            this.offsetValue = offsetValue;
            this.offsetType = offsetType;
        }
    }
}

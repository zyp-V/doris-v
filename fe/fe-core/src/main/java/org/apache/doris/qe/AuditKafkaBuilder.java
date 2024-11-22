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

package org.apache.doris.qe;

import org.apache.doris.common.AuditKafka;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.plugin.audit.AuditEvent;
import org.apache.doris.plugin.audit.AuditEvent.EventType;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// A builtin KafkaAudit plugin, registered when FE start
// it will receive "AFTER_QUERY" AuditEventy and print it as a json log to kafka topic
public class AuditKafkaBuilder extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditKafkaBuilder.class);

    private PluginInfo pluginInfo;
    private AuditKafka auditKafka;
    private EventType auditEventType;

    public AuditKafkaBuilder(String cluster, String topic, EventType eventType) throws IllegalArgumentException {
        String pluginName = PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditKafkaBuilder";
        String suffix = "";
        auditEventType = eventType;
        if (auditEventType == EventType.AFTER_QUERY) {
            suffix = "ForQuery";
        } else if (auditEventType == EventType.STREAM_LOAD_FINISH) {
            suffix = "ForStreamLoad";
        }
        pluginInfo = new PluginInfo(pluginName + suffix, PluginType.AUDIT, "builtin kafka audit logger",
                DigitalVersion.fromString("2.0.0"), DigitalVersion.fromString("11.0.1"),
                AuditKafkaBuilder.class.getName(), null, null);
        if (Strings.isNullOrEmpty(cluster) || Strings.isNullOrEmpty(topic)) {
            throw new IllegalArgumentException("Kafka config is null");
        }
        auditKafka = new AuditKafka(cluster, topic);
        // start Kafka work thread
        auditKafka.start("AuditToKafka" + suffix);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public boolean eventFilter(EventType type) {
        return type == auditEventType;
    }

    @Override
    public void exec(AuditEvent event) {
        try {
            auditKafka.handleKafkaLog(event);
        } catch (Exception e) {
            LOG.debug("failed to process building audit kafka event", e);
        }
    }
}

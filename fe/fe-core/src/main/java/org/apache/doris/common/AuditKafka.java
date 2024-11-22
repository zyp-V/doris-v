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

package org.apache.doris.common;

import org.apache.doris.plugin.audit.AuditEvent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AuditKafka {
    private static final Logger LOG = LogManager.getLogger(AuditKafka.class);

    private KafkaProducer<String, String> producer;

    private BlockingQueue<AuditEvent> messageQueue = Queues.newLinkedBlockingDeque(10000);
    private ExecutorService workerThreads = Executors.newFixedThreadPool(Config.audit_query_log_kafka_parallelism);
    private ObjectMapper mapper = new ObjectMapper();

    private volatile boolean isStopped = false;
    private String cluster;
    private String topic;

    public AuditKafka(String cluster, String topic) throws RuntimeException {
        this.cluster = cluster;
        this.topic = topic;
        // start a kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.CLUSTER_NAME_CONFIG, cluster);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // time out is 1 min
        props.put("start.timeout", 60000);
        // support ZTI token
        props.put(CommonClientConfigs.ENABLE_ZTI_TOKEN, true);
        producer = new KafkaProducer<String, String>(props);
    }

    public void start(String name) {
        for (int i = 0; i < Config.audit_query_log_kafka_parallelism; i++) {
            workerThreads.submit(new Worker());
        }
    }

    public void stop() {
        isStopped = true;
        workerThreads.shutdown();
        // It's over
        producer.close();
    }

    public void handleKafkaLog(AuditEvent auditEvent) {
        // we think that kafka shouldn't affect the query speed
        boolean result = messageQueue.offer(auditEvent);
        if (result == false) {
            LOG.warn("Failed to send audit log for query {} to kafka: queue is full", auditEvent.queryId);
        }

    }

    public void log(AuditEvent auditEvent) throws IOException {
        String message = mapper.writeValueAsString(auditEvent);
        producer.send(new ProducerRecord<String, String>(topic, message), new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    LOG.warn("Failed to send audit log for query {} to kafka: {}", auditEvent.queryId, e.getMessage());
                }
            }
        });
    }


    public class Worker implements Runnable {
        @Override
        public void run() {
            AuditEvent msgKafka;
            while (!isStopped) {
                try {
                    msgKafka = messageQueue.poll(5, TimeUnit.SECONDS);
                    if (msgKafka == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when getting auditKafka message from kafkaData queue, ignore", e);
                    continue;
                }

                try {
                    log(msgKafka);
                } catch (Exception e) {
                    LOG.warn("Failed to send audit log for query/stream load {} to kafka: {}", msgKafka.queryId,
                            e.getMessage());
                }
            }
        }
    }
}

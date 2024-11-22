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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import com.bytedance.storage.tos.Tos;
import com.bytedance.storage.tos.TosClient;
import com.bytedance.storage.tos.TosException;
import com.bytedance.storage.tos.TosProperty;
import com.bytedance.storage.tos.TransportProperty;
import com.bytedance.storage.tos.model.PutObjectRequest;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProfileToTos {
    private static final Logger LOG = LogManager.getLogger(ProfileToTos.class);

    private String tosBucket;
    private String tosAccessKey;
    private String tosCluster;

    private TosProperty property;
    private TosClient client;

    private BlockingQueue<Pair<String, String>> messageQueue = Queues.newLinkedBlockingDeque(500);
    private ExecutorService workerThreads = Executors.newFixedThreadPool(Config.audit_log_profile_tos_parallelism);

    private volatile boolean isStopped = false;

    public ProfileToTos(String bucket, String accessKey, String cluster) {
        tosBucket = bucket;
        tosAccessKey = accessKey;
        tosCluster = cluster;
    }

    public void start() {
        property = new TosProperty()
                .setAccessKey(tosAccessKey)
                .setBucket(tosBucket)
                .setCluster(tosCluster)
                .setTimeout(10);
        // Timeout : Tos write/read timeout
        // we will only use write, timeout is 10s.
        client = Tos.create(property)
                .withDefaultTransport(new TransportProperty()
                        .setKeepAliveTimeout(10 * 60))
                .build();
        LOG.info("profile to tos is starting...");
        // Keep Alive Time out : If no write, connect will be closed.
        // If connect has been closed, putObject will create new connect.
        // If 10 min has no slow query to write, we think it is too excellent
        // to send profile now, connect will be closed.
        for (int i = 0; i < Config.audit_log_profile_tos_parallelism; i++) {
            workerThreads.submit(new Worker());
        }
    }

    public void stop() {
        isStopped = true;
        workerThreads.shutdown();
    }

    public void handleProfile(String queryId, String profile) {
        // we think that TOS shouldn't affect the query speed
        boolean result = messageQueue.offer(Pair.of(queryId, profile));
        if (result == false) {
            LOG.warn("Failed to send profile for query {} to TOS: queue is full", queryId);
        }
    }

    private void log(String key, String data) throws TosException {
        PutObjectRequest request = new PutObjectRequest().setKey(key)
                .setData(data.getBytes())
                .setHeaders(Collections.singletonMap("Content-Type", "text/plain; charset=utf-8"));
        client.putObject(request);
        // we needn't care result
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            Pair<String, String> msgTos;
            while (!isStopped) {
                try {
                    msgTos = messageQueue.poll(5, TimeUnit.SECONDS);
                    if (msgTos == null) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    LOG.debug("encounter exception when getting profile from TosData queue, ignore", e);
                    continue;
                }

                try {
                    log(msgTos.first, msgTos.second);
                } catch (Exception e) {
                    LOG.warn("Failed to send profile for query {} to TOS: {}", msgTos.first, e.toString());
                }
            }
        }
    }
}

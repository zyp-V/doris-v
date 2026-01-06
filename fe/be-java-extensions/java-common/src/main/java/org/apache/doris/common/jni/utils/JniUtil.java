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

package org.apache.doris.common.jni.utils;

import org.apache.doris.common.exception.InternalException;
import org.apache.doris.thrift.TGetJMXJsonResponse;
import org.apache.doris.thrift.TGetJvmMemoryMetricsResponse;
import org.apache.doris.thrift.TGetJvmThreadsInfoRequest;
import org.apache.doris.thrift.TGetJvmThreadsInfoResponse;
import org.apache.doris.thrift.TJvmMemoryPool;
import org.apache.doris.thrift.TJvmThreadInfo;

import com.google.common.base.Joiner;
import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

/**
 * Utility class with methods intended for JNI clients
 */
public class JniUtil {
    private static final TBinaryProtocol.Factory protocolFactory_ = new TBinaryProtocol.Factory();
    private static final Logger log = LoggerFactory.getLogger(JniUtil.class);

    static {
        String logPath = System.getProperty("logPath");
        // Fallback to DORIS_HOME if logPath is not set
        if (logPath == null || logPath.isEmpty()) {
            String dorisHome = System.getenv("DORIS_HOME");
            if (dorisHome != null) {
                logPath = dorisHome + "/log/jni.log";
            }
        }
        System.out.println("JniUtil: inferred logPath = " + logPath);

        // 1. Configure java.util.logging (JUL)
        try {
            java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
            Handler[] handlers = rootLogger.getHandlers();
            for (Handler handler : handlers) {
                if (handler instanceof ConsoleHandler) {
                    rootLogger.removeHandler(handler);
                }
            }

            if (logPath != null && !logPath.isEmpty()) {
                String julLogPath = logPath + ".jul";
                // 50MB limit, 2 files, append mode
                FileHandler fileHandler = new FileHandler(julLogPath, 50 * 1024 * 1024, 2, true);
                fileHandler.setFormatter(new SimpleFormatter());
                rootLogger.addHandler(fileHandler);
                System.out.println("JniUtil: JUL logs redirected to " + julLogPath);
            }
            // Silence the noisy security component causing NoClassDefFoundError
            java.util.logging.Logger.getLogger("org.byted.security").setLevel(Level.OFF);
        } catch (Exception e) {
            System.err.println("JniUtil: Failed to configure java.util.logging: " + e.getMessage());
            e.printStackTrace();
        }

        // 2. Configure Log4j
        // Manually load configuration from DORIS_HOME/conf/be_jni_log4j.properties
        try {
            String dorisHome = System.getenv("DORIS_HOME");
            if (dorisHome != null) {
                String log4jConfigPath = dorisHome + "/conf/be_jni_log4j.properties";
                File log4jConfigFile = new File(log4jConfigPath);
                if (log4jConfigFile.exists()) {
                    PropertyConfigurator.configure(log4jConfigPath);
                    System.out.println("JniUtil: Log4j configured using " + log4jConfigPath);
                } else {
                    System.err.println("JniUtil: Log4j configuration file not found at: " + log4jConfigPath);
                }
            } else {
                System.err.println("JniUtil: DORIS_HOME env var not set, skipping Log4j config");
            }
        } catch (Exception e) {
            System.err.println("JniUtil: Failed to configure Log4j: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Initializes the JvmPauseMonitor instance.
     */
    public static void initPauseMonitor(long deadlockCheckIntervalS) {
        JvmPauseMonitor.INSTANCE.initPauseMonitor(deadlockCheckIntervalS);
    }

    /**
     * Returns a formatted string containing the simple exception name and the
     * exception message without the full stack trace. Includes the
     * the chain of causes each in a separate line.
     */
    public static String throwableToString(Throwable t) {
        StringWriter output = new StringWriter();
        output.write(String.format("%s: %s", t.getClass().getSimpleName(),
                t.getMessage()));
        // Follow the chain of exception causes and print them as well.
        Throwable cause = t;
        while ((cause = cause.getCause()) != null) {
            output.write(String.format(" | CAUSED BY: %s: %s",
                    cause.getClass().getSimpleName(), cause.getMessage()));
        }
        return output.toString();
    }

    /**
     * Returns the stack trace of the Throwable object.
     */
    public static String throwableToStackTrace(Throwable t) {
        Writer output = new StringWriter();
        t.printStackTrace(new PrintWriter(output));
        return output.toString();
    }

    /**
     * Serializes input into a byte[] using the default protocol factory.
     */
    public static <T extends TBase<?, ?>> byte[] serializeToThrift(T input) throws InternalException {
        try {
            TSerializer serializer = new TSerializer(protocolFactory_);
            return serializer.serialize(input);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
    }

    /**
     * Serializes input into a byte[] using a given protocol factory.
     */
    public static <T extends TBase<?, ?>, F extends TProtocolFactory> byte[] serializeToThrift(
            T input, F protocolFactory) throws InternalException {
        try {
            TSerializer serializer = new TSerializer(protocolFactory);
            return serializer.serialize(input);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
    }

    public static <T extends TBase<?, ?>> void deserializeThrift(
            T result, byte[] thriftData) throws InternalException {
        deserializeThrift(protocolFactory_, result, thriftData);
    }

    /**
     * Deserialize a serialized form of a Thrift data structure to its object form.
     */
    public static <T extends TBase<?, ?>, F extends TProtocolFactory> void deserializeThrift(
            F protocolFactory, T result, byte[] thriftData) throws InternalException {
        // TODO: avoid creating deserializer for each query?
        try {
            TDeserializer deserializer = new TDeserializer(protocolFactory);
            deserializer.deserialize(result, thriftData);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
    }

    /**
     * Collect the JVM's memory statistics into a thrift structure for translation into
     * Doris metrics by the backend. A synthetic 'total' memory pool is included with
     * aggregate statistics for all real pools. Metrics for the JvmPauseMonitor
     * and Garbage Collection are also included.
     */
    public static byte[] getJvmMemoryMetrics() throws InternalException {
        TGetJvmMemoryMetricsResponse jvmMetrics = new TGetJvmMemoryMetricsResponse();
        jvmMetrics.setMemoryPools(new ArrayList<TJvmMemoryPool>());
        TJvmMemoryPool totalUsage = new TJvmMemoryPool();

        totalUsage.setName("total");
        jvmMetrics.getMemoryPools().add(totalUsage);

        for (MemoryPoolMXBean memBean : ManagementFactory.getMemoryPoolMXBeans()) {
            TJvmMemoryPool usage = new TJvmMemoryPool();
            MemoryUsage beanUsage = memBean.getUsage();
            usage.setCommitted(beanUsage.getCommitted());
            usage.setInit(beanUsage.getInit());
            usage.setMax(beanUsage.getMax());
            usage.setUsed(beanUsage.getUsed());
            usage.setName(memBean.getName());

            totalUsage.committed += beanUsage.getCommitted();
            totalUsage.init += beanUsage.getInit();
            totalUsage.max += beanUsage.getMax();
            totalUsage.used += beanUsage.getUsed();

            MemoryUsage peakUsage = memBean.getPeakUsage();
            usage.setPeakCommitted(peakUsage.getCommitted());
            usage.setPeakInit(peakUsage.getInit());
            usage.setPeakMax(peakUsage.getMax());
            usage.setPeakUsed(peakUsage.getUsed());

            totalUsage.peak_committed += peakUsage.getCommitted();
            totalUsage.peak_init += peakUsage.getInit();
            totalUsage.peak_max += peakUsage.getMax();
            totalUsage.peak_used += peakUsage.getUsed();

            jvmMetrics.getMemoryPools().add(usage);
        }

        // Populate heap usage
        MemoryMXBean mBean = ManagementFactory.getMemoryMXBean();
        TJvmMemoryPool heap = new TJvmMemoryPool();
        MemoryUsage heapUsage = mBean.getHeapMemoryUsage();
        heap.setCommitted(heapUsage.getCommitted());
        heap.setInit(heapUsage.getInit());
        heap.setMax(heapUsage.getMax());
        heap.setUsed(heapUsage.getUsed());
        heap.setName("heap");
        heap.setPeakCommitted(0);
        heap.setPeakInit(0);
        heap.setPeakMax(0);
        heap.setPeakUsed(0);
        jvmMetrics.getMemoryPools().add(heap);

        // Populate non-heap usage
        TJvmMemoryPool nonHeap = new TJvmMemoryPool();
        MemoryUsage nonHeapUsage = mBean.getNonHeapMemoryUsage();
        nonHeap.setCommitted(nonHeapUsage.getCommitted());
        nonHeap.setInit(nonHeapUsage.getInit());
        nonHeap.setMax(nonHeapUsage.getMax());
        nonHeap.setUsed(nonHeapUsage.getUsed());
        nonHeap.setName("non-heap");
        nonHeap.setPeakCommitted(0);
        nonHeap.setPeakInit(0);
        nonHeap.setPeakMax(0);
        nonHeap.setPeakUsed(0);
        jvmMetrics.getMemoryPools().add(nonHeap);

        // Populate JvmPauseMonitor metrics
        jvmMetrics.setGcNumWarnThresholdExceeded(
                JvmPauseMonitor.INSTANCE.getNumGcWarnThresholdExceeded());
        jvmMetrics.setGcNumInfoThresholdExceeded(
                JvmPauseMonitor.INSTANCE.getNumGcInfoThresholdExceeded());
        jvmMetrics.setGcTotalExtraSleepTimeMillis(
                JvmPauseMonitor.INSTANCE.getTotalGcExtraSleepTime());

        // And Garbage Collector metrics
        long gcCount = 0;
        long gcTimeMillis = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += bean.getCollectionCount();
            gcTimeMillis += bean.getCollectionTime();
        }
        jvmMetrics.setGcCount(gcCount);
        jvmMetrics.setGcTimeMillis(gcTimeMillis);

        return serializeToThrift(jvmMetrics, protocolFactory_);
    }

    /**
     * Get information about the live JVM threads.
     */
    public static byte[] getJvmThreadsInfo(byte[] argument) throws InternalException {
        TGetJvmThreadsInfoRequest request = new TGetJvmThreadsInfoRequest();
        JniUtil.deserializeThrift(protocolFactory_, request, argument);
        TGetJvmThreadsInfoResponse response = new TGetJvmThreadsInfoResponse();
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        response.setTotalThreadCount(threadBean.getThreadCount());
        response.setDaemonThreadCount(threadBean.getDaemonThreadCount());
        response.setPeakThreadCount(threadBean.getPeakThreadCount());
        if (request.get_complete_info) {
            for (ThreadInfo threadInfo : threadBean.dumpAllThreads(true, true)) {
                TJvmThreadInfo tThreadInfo = new TJvmThreadInfo();
                long id = threadInfo.getThreadId();
                tThreadInfo.setSummary(threadInfo.toString());
                tThreadInfo.setCpuTimeInNs(threadBean.getThreadCpuTime(id));
                tThreadInfo.setUserTimeInNs(threadBean.getThreadUserTime(id));
                tThreadInfo.setBlockedCount(threadInfo.getBlockedCount());
                tThreadInfo.setBlockedTimeInMs(threadInfo.getBlockedTime());
                tThreadInfo.setIsInNative(threadInfo.isInNative());
                response.addToThreads(tThreadInfo);
            }
        }
        return serializeToThrift(response, protocolFactory_);
    }

    public static byte[] getJMXJson() throws InternalException {
        TGetJMXJsonResponse response = new TGetJMXJsonResponse(JMXJsonUtil.getJMXJson());
        return serializeToThrift(response, protocolFactory_);
    }

    /**
     * Get Java version, input arguments and system properties.
     */
    public static String getJavaVersion() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        StringBuilder sb = new StringBuilder();
        sb.append("Java Input arguments:\n");
        sb.append(Joiner.on(" ").join(runtime.getInputArguments()));
        sb.append("\nJava System properties:\n");
        for (Map.Entry<String, String> entry : runtime.getSystemProperties().entrySet()) {
            sb.append(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        return sb.toString();
    }

    private static class HotSpotDiagnosticMXBeanHolder {

        private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

        private static volatile HotSpotDiagnosticMXBean hotSpotDiagnostic;

        public static HotSpotDiagnosticMXBean getHotSpotDiagnostic() {
            if (hotSpotDiagnostic == null) {
                synchronized (HotSpotDiagnosticMXBeanHolder.class) {
                    if (hotSpotDiagnostic == null) {
                        hotSpotDiagnostic = getHotSpotDiagnosticMXBean();
                    }
                }
            }
            return hotSpotDiagnostic;
        }

        private static HotSpotDiagnosticMXBean getHotSpotDiagnosticMXBean() {
            try {
                return ManagementFactory.newPlatformMXBeanProxy(
                        ManagementFactory.getPlatformMBeanServer(),
                        HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static boolean dumpHeap(String path) throws IOException {
        HotSpotDiagnosticMXBean hotSpot = HotSpotDiagnosticMXBeanHolder.getHotSpotDiagnostic();
        hotSpot.dumpHeap(path, false);
        return true;
    }
}

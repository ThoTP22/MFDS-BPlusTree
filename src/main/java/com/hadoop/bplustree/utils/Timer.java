package com.hadoop.bplustree.utils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Enhanced Execution Time Measurement Utility
 * Thread-safe version with extended reporting capabilities
 */
public class Timer {
    private static final Logger LOG = Logger.getLogger(Timer.class.getName());
    private static final Map<String, Long> startTimes = new ConcurrentHashMap<>();
    private static final Map<String, Long> endTimes = new ConcurrentHashMap<>();
    private static final Map<String, List<Long>> measurements = new ConcurrentHashMap<>();

    /**
     * Class for storing timing data
     */
    private static class TimingData {
        long startTime;
        long endTime;
        long duration;
        int count;
        double average;
        long min;
        long max;
        double standardDeviation;
    }

    /**
     * Starts timing measurement
     */
    public static void start(String phase) {
        startTimes.put(phase, System.currentTimeMillis());
        measurements.computeIfAbsent(phase, k -> new ArrayList<>());
    }

    /**
     * Ends timing measurement
     */
    public static long end(String phase) {
        long endTime = System.currentTimeMillis();
        endTimes.put(phase, endTime);
        
        Long startTime = startTimes.get(phase);
        if (startTime != null) {
            long duration = endTime - startTime;
            measurements.get(phase).add(duration);
            return duration;
        }
        return -1;
    }

    /**
     * Gets execution time
     */
    public static long getTime(String phase) {
        Long endTime = endTimes.get(phase);
        Long startTime = startTimes.get(phase);
        if (endTime != null && startTime != null) {
            return endTime - startTime;
        }
        return -1;
    }

    /**
     * Checks if phase has been started
     */
    public static boolean isStarted(String phase) {
        return startTimes.containsKey(phase);
    }

    /**
     * Checks if phase has been completed
     */
    public static boolean isCompleted(String phase) {
        return endTimes.containsKey(phase);
    }

    /**
     * Prints report
     */
    public static void report() {
        // Aggregate statistics
        Map<String, TimingData> stats = new HashMap<>();
        
        for (String phase : measurements.keySet()) {
            List<Long> times = measurements.get(phase);
            if (!times.isEmpty()) {
                TimingData data = new TimingData();
                data.count = times.size();
                data.min = Collections.min(times);
                data.max = Collections.max(times);
                data.average = times.stream().mapToLong(Long::longValue).average().orElse(0);
                
                // Calculate standard deviation
                double variance = times.stream()
                    .mapToDouble(time -> Math.pow(time - data.average, 2))
                    .average()
                    .orElse(0);
                data.standardDeviation = Math.sqrt(variance);
                
                stats.put(phase, data);
            }
        }

        // Print detailed information
        LOG.info("======= Execution Time Report =======");
        for (Map.Entry<String, TimingData> entry : stats.entrySet()) {
            TimingData data = entry.getValue();
            LOG.info(String.format("Phase: %s", entry.getKey()));
            LOG.info(String.format("  Count: %d", data.count));
            LOG.info(String.format("  Average: %.2f ms", data.average));
            LOG.info(String.format("  Min: %d ms", data.min));
            LOG.info(String.format("  Max: %d ms", data.max));
            LOG.info(String.format("  Std Dev: %.2f ms", data.standardDeviation));
        }

        // JVM information
        Runtime runtime = Runtime.getRuntime();
        LOG.info("======= JVM Information =======");
        LOG.info(String.format("Max Memory: %.2f MB", runtime.maxMemory() / (1024.0 * 1024)));
        LOG.info(String.format("Total Memory: %.2f MB", runtime.totalMemory() / (1024.0 * 1024)));
        LOG.info(String.format("Free Memory: %.2f MB", runtime.freeMemory() / (1024.0 * 1024)));
        LOG.info("===============================");
    }

    /**
     * Saves report to file with enhanced formatting
     */
    public static void save(String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            // Aggregate statistics
            Map<String, TimingData> stats = new HashMap<>();
            
            for (String phase : measurements.keySet()) {
                List<Long> times = measurements.get(phase);
                if (!times.isEmpty()) {
                    TimingData data = new TimingData();
                    data.count = times.size();
                    data.min = Collections.min(times);
                    data.max = Collections.max(times);
                    data.average = times.stream().mapToLong(Long::longValue).average().orElse(0);
                    
                    double variance = times.stream()
                        .mapToDouble(time -> Math.pow(time - data.average, 2))
                        .average()
                        .orElse(0);
                    data.standardDeviation = Math.sqrt(variance);
                    
                    stats.put(phase, data);
                }
            }

            // Print detailed information
            writer.write("======= Execution Time Report =======\n");
            writer.write("Generated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n\n");
            
            for (Map.Entry<String, TimingData> entry : stats.entrySet()) {
                TimingData data = entry.getValue();
                writer.write(String.format("Phase: %s\n", entry.getKey()));
                writer.write(String.format("  Count: %d\n", data.count));
                writer.write(String.format("  Average: %.2f ms\n", data.average));
                writer.write(String.format("  Min: %d ms\n", data.min));
                writer.write(String.format("  Max: %d ms\n", data.max));
                writer.write(String.format("  Std Dev: %.2f ms\n", data.standardDeviation));
                writer.write("\n");
            }

            // JVM information
            Runtime runtime = Runtime.getRuntime();
            writer.write("======= JVM Information =======\n");
            writer.write(String.format("Max Memory: %.2f MB\n", runtime.maxMemory() / (1024.0 * 1024)));
            writer.write(String.format("Total Memory: %.2f MB\n", runtime.totalMemory() / (1024.0 * 1024)));
            writer.write(String.format("Free Memory: %.2f MB\n", runtime.freeMemory() / (1024.0 * 1024)));
            writer.write("================================\n");
        }
    }

    /**
     * Saves report in CSV format for analysis
     */
    public static void saveCSV(String filename) throws IOException {
        File file = new File(filename);
        boolean isNewFile = !file.exists();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            // Write header if new file
            if (isNewFile) {
                writer.write("Phase,Count,Average,Min,Max,StdDev\n");
            }

            // Write data
            for (String phase : measurements.keySet()) {
                List<Long> times = measurements.get(phase);
                if (!times.isEmpty()) {
                    double average = times.stream().mapToLong(Long::longValue).average().orElse(0);
                    long min = Collections.min(times);
                    long max = Collections.max(times);
                    
                    double variance = times.stream()
                        .mapToDouble(time -> Math.pow(time - average, 2))
                        .average()
                        .orElse(0);
                    double stdDev = Math.sqrt(variance);
                    
                    writer.write(String.format("%s,%d,%.2f,%d,%d,%.2f\n",
                        phase, times.size(), average, min, max, stdDev));
                }
            }
        }
    }

    /**
     * Clears all timers
     */
    public static void clear() {
        startTimes.clear();
        endTimes.clear();
        measurements.clear();
    }
}
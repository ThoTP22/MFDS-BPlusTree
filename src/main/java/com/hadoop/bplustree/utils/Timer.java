package com.hadoop.bplustree.utils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tiện ích đo thời gian thực thi (Cải tiến)
 * Phiên bản thread-safe với nhiều tính năng báo cáo hơn
 */
public class Timer {
    private static final Logger LOG = Logger.getLogger(Timer.class.getName());
    private static final ConcurrentHashMap<String, TimerData> timers = new ConcurrentHashMap<>();
    private static final Object reportLock = new Object();

    /**
     * Lớp lưu trữ dữ liệu thời gian
     */
    private static class TimerData {
        private final long startTime;
        private long endTime = -1;
        private final long memoryAtStart;
        private long memoryAtEnd = -1;
        private final Thread startThread;
        private Thread endThread;

        public TimerData() {
            this.startTime = System.currentTimeMillis();
            this.memoryAtStart = getUsedMemory();
            this.startThread = Thread.currentThread();
        }

        public void end() {
            this.endTime = System.currentTimeMillis();
            this.memoryAtEnd = getUsedMemory();
            this.endThread = Thread.currentThread();
        }

        public long getDuration() {
            if (endTime == -1) {
                return System.currentTimeMillis() - startTime;
            }
            return endTime - startTime;
        }

        public long getMemoryUsage() {
            if (memoryAtEnd == -1) {
                return getUsedMemory() - memoryAtStart;
            }
            return memoryAtEnd - memoryAtStart;
        }

        public boolean isCompleted() {
            return endTime != -1;
        }

        public String getStartThreadName() {
            return startThread.getName();
        }

        public String getEndThreadName() {
            return endThread != null ? endThread.getName() : "N/A";
        }

        private static long getUsedMemory() {
            Runtime runtime = Runtime.getRuntime();
            return runtime.totalMemory() - runtime.freeMemory();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            long duration = getDuration();

            sb.append(String.format("Duration: %dms (%.2fs)", duration, duration / 1000.0));

            if (duration >= 60000) {
                long minutes = TimeUnit.MILLISECONDS.toMinutes(duration);
                long seconds = TimeUnit.MILLISECONDS.toSeconds(duration) % 60;
                sb.append(String.format(" [%d min, %d sec]", minutes, seconds));
            }

            long memUsage = getMemoryUsage() / (1024 * 1024);
            sb.append(String.format(", Memory: %d MB", memUsage));

            sb.append(", Threads: ").append(getStartThreadName());
            if (isCompleted() && !getStartThreadName().equals(getEndThreadName())) {
                sb.append(" → ").append(getEndThreadName());
            }

            return sb.toString();
        }
    }

    /**
     * Bắt đầu đo thời gian
     */
    public static void start(String phase) {
        TimerData timer = new TimerData();
        timers.put(phase, timer);
        LOG.info("Started: " + phase);
    }

    /**
     * Kết thúc đo thời gian
     */
    public static long end(String phase) {
        TimerData timer = timers.get(phase);
        if (timer == null) {
            LOG.warning("No start time for: " + phase);
            return 0;
        }

        timer.end();
        long duration = timer.getDuration();

        LOG.info(String.format("%s: %s", phase, timer.toString()));

        return duration;
    }

    /**
     * Lấy thời gian thực thi
     */
    public static long get(String phase) {
        TimerData timer = timers.get(phase);
        return timer != null ? timer.getDuration() : 0L;
    }

    /**
     * Kiểm tra xem phase đã được bắt đầu chưa
     */
    public static boolean isStarted(String phase) {
        return timers.containsKey(phase);
    }

    /**
     * Kiểm tra xem phase đã kết thúc chưa
     */
    public static boolean isCompleted(String phase) {
        TimerData timer = timers.get(phase);
        return timer != null && timer.isCompleted();
    }

    /**
     * In báo cáo
     */
    public static void report() {
        synchronized (reportLock) {
            LOG.info("=== Timing Report ===");

            // Thống kê tổng hợp
            int totalPhases = timers.size();
            int completedPhases = 0;
            long totalDuration = 0;
            long maxDuration = 0;
            String longestPhase = "";

            for (Map.Entry<String, TimerData> entry : timers.entrySet()) {
                TimerData timer = entry.getValue();

                if (timer.isCompleted()) {
                    completedPhases++;

                    long duration = timer.getDuration();
                    totalDuration += duration;

                    if (duration > maxDuration) {
                        maxDuration = duration;
                        longestPhase = entry.getKey();
                    }
                }
            }

            // In thông tin chi tiết
            List<Map.Entry<String, TimerData>> sortedEntries = new ArrayList<>(timers.entrySet());
            sortedEntries.sort(Map.Entry.comparingByKey());

            for (Map.Entry<String, TimerData> entry : sortedEntries) {
                LOG.info(String.format("%-20s: %s",
                        entry.getKey(), entry.getValue().toString()));
            }

            LOG.info("========================");
            LOG.info(String.format("Summary: %d/%d phases completed", completedPhases, totalPhases));
            LOG.info(String.format("Total time: %dms (%.2fs)", totalDuration, totalDuration / 1000.0));
            if (!longestPhase.isEmpty()) {
                LOG.info(String.format("Longest phase: %s (%dms, %.2fs)",
                        longestPhase, maxDuration, maxDuration / 1000.0));
            }

            // Thông tin JVM
            Runtime runtime = Runtime.getRuntime();
            LOG.info(String.format("JVM Memory: used=%dMB, free=%dMB, max=%dMB",
                    (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024),
                    runtime.freeMemory() / (1024 * 1024),
                    runtime.maxMemory() / (1024 * 1024)));
        }
    }

    /**
     * Lưu báo cáo ra file với định dạng phong phú hơn
     */
    public static void save(String file) {
        synchronized (reportLock) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                String separator = "=".repeat(50);

                writer.write(separator);
                writer.newLine();
                writer.write("=== Timing Report - " + timestamp + " ===");
                writer.newLine();
                writer.write(separator);
                writer.newLine();

                // Thống kê tổng hợp
                int totalPhases = timers.size();
                int completedPhases = 0;
                long totalDuration = 0;
                long maxDuration = 0;
                String longestPhase = "";

                for (Map.Entry<String, TimerData> entry : timers.entrySet()) {
                    TimerData timer = entry.getValue();

                    if (timer.isCompleted()) {
                        completedPhases++;

                        long duration = timer.getDuration();
                        totalDuration += duration;

                        if (duration > maxDuration) {
                            maxDuration = duration;
                            longestPhase = entry.getKey();
                        }
                    }
                }

                // In thông tin chi tiết
                writer.write("DETAILED TIMINGS:");
                writer.newLine();
                writer.write("-".repeat(30));
                writer.newLine();

                List<Map.Entry<String, TimerData>> sortedEntries = new ArrayList<>(timers.entrySet());
                sortedEntries.sort(Map.Entry.comparingByKey());

                for (Map.Entry<String, TimerData> entry : sortedEntries) {
                    writer.write(String.format("%-20s: %s",
                            entry.getKey(), entry.getValue().toString()));
                    writer.newLine();
                }

                writer.write(separator);
                writer.newLine();
                writer.write("SUMMARY:");
                writer.newLine();
                writer.write("-".repeat(30));
                writer.newLine();
                writer.write(String.format("Phases: %d/%d completed", completedPhases, totalPhases));
                writer.newLine();
                writer.write(String.format("Total time: %dms (%.2fs)", totalDuration, totalDuration / 1000.0));
                writer.newLine();

                if (totalDuration >= 60000) {
                    long hours = TimeUnit.MILLISECONDS.toHours(totalDuration);
                    long minutes = TimeUnit.MILLISECONDS.toMinutes(totalDuration) % 60;
                    long seconds = TimeUnit.MILLISECONDS.toSeconds(totalDuration) % 60;
                    writer.write(String.format("Total time (human): %d hours, %d minutes, %d seconds",
                            hours, minutes, seconds));
                    writer.newLine();
                }

                if (!longestPhase.isEmpty()) {
                    writer.write(String.format("Longest phase: %s (%dms, %.2fs)",
                            longestPhase, maxDuration, maxDuration / 1000.0));
                    writer.newLine();
                }

                // Thông tin JVM
                Runtime runtime = Runtime.getRuntime();
                writer.write(String.format("JVM Memory: used=%dMB, free=%dMB, max=%dMB",
                        (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024),
                        runtime.freeMemory() / (1024 * 1024),
                        runtime.maxMemory() / (1024 * 1024)));
                writer.newLine();

                writer.write(separator);
                writer.newLine();
                writer.newLine();

                LOG.info("Report saved to: " + file);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Failed to save report: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Lưu báo cáo định dạng CSV để phân tích
     */
    public static void saveCSV(String file) {
        synchronized (reportLock) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

                // Viết header nếu file mới
                File csvFile = new File(file);
                boolean isNewFile = !csvFile.exists() || csvFile.length() == 0;

                if (isNewFile) {
                    writer.write("Timestamp,Phase,Duration(ms),Duration(s),MemoryUsage(MB),StartThread,EndThread,Completed");
                    writer.newLine();
                }

                // Viết dữ liệu
                for (Map.Entry<String, TimerData> entry : timers.entrySet()) {
                    TimerData timer = entry.getValue();

                    writer.write(String.format("%s,%s,%d,%.2f,%d,%s,%s,%s",
                            timestamp,
                            entry.getKey(),
                            timer.getDuration(),
                            timer.getDuration() / 1000.0,
                            timer.getMemoryUsage() / (1024 * 1024),
                            timer.getStartThreadName(),
                            timer.isCompleted() ? timer.getEndThreadName() : "N/A",
                            timer.isCompleted() ? "Yes" : "No"));
                    writer.newLine();
                }

                LOG.info("CSV report saved to: " + file);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Failed to save CSV report: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Xóa tất cả timers
     */
    public static void reset() {
        timers.clear();
        LOG.info("All timers have been reset");
    }
}
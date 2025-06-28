package com.hadoop.bplustree.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Enhanced Partition Information Management System
 */
public class PartitionInfo {

    private static final Logger LOG = Logger.getLogger(PartitionInfo.class.getName());
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000;

    /**
     * Saves partition information with retry mechanism
     */
    public static void save(long[] points, Path path, Configuration conf) throws IOException {
        if (points == null) {
            throw new IllegalArgumentException("Partition points array cannot be null");
        }

        int retries = 0;
        boolean success = false;
        Exception lastException = null;

        while (!success && retries < MAX_RETRIES) {
            try {
                FileSystem fs = FileSystem.get(conf);

                // Create parent directory if needed
                Path parent = path.getParent();
                if (parent != null && !fs.exists(parent)) {
                    fs.mkdirs(parent);
                }

                // Save with integrity check support
                FSDataOutputStream out = fs.create(path);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

                try {
                    // Save header with point count information
                    writer.write("# Number of partition points: " + points.length);
                    writer.newLine();
                    writer.write("# Format: point_index point_value");
                    writer.newLine();
                    writer.write("# Generated on: " + new java.util.Date());
                    writer.newLine();

                    for (int i = 0; i < points.length; i++) {
                        writer.write(i + "\t" + points[i]);
                        writer.newLine();
                    }

                    // Add simple checksum
                    long checksum = calculateChecksum(points);
                    writer.write("# CHECKSUM: " + checksum);
                    writer.newLine();

                    success = true;
                    LOG.info("Saved " + points.length + " partition points to " + path + " with checksum " + checksum);
                } finally {
                    writer.close();
                }
            } catch (IOException e) {
                lastException = e;
                LOG.log(Level.WARNING, "Failed to save partition points (attempt " + (retries + 1) + "): " + e.getMessage(), e);
                retries++;

                if (retries < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * retries);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting to retry", ie);
                    }
                }
            }
        }

        if (!success) {
            throw new IOException("Failed to save partition points after " + MAX_RETRIES + " attempts", lastException);
        }
    }

    /**
     * Loads partition information with retry mechanism and integrity verification
     */
    public static long[] load(Path path, Configuration conf) throws IOException {
        int retries = 0;
        List<Long> points = null;
        long expectedChecksum = -1;
        Exception lastException = null;

        while (points == null && retries < MAX_RETRIES) {
            try {
                FileSystem fs = FileSystem.get(conf);
                if (!fs.exists(path)) {
                    throw new FileNotFoundException("Partition points file not found: " + path);
                }

                points = new ArrayList<>();
                boolean foundChecksum = false;

                try (FSDataInputStream in = fs.open(path);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();

                        // Skip empty lines or comments (except checksum line)
                        if (line.isEmpty() || (line.startsWith("#") && !line.startsWith("# CHECKSUM:"))) {
                            continue;
                        }

                        // Read checksum if present
                        if (line.startsWith("# CHECKSUM:")) {
                            expectedChecksum = Long.parseLong(line.substring("# CHECKSUM:".length()).trim());
                            foundChecksum = true;
                            continue;
                        }

                        // Process data line
                        String[] parts = line.split("\\s+");

                        // If format is index\tvalue, take the value part
                        if (parts.length >= 2) {
                            try {
                                points.add(Long.parseLong(parts[1]));
                            } catch (NumberFormatException e) {
                                LOG.warning("Invalid format in line: " + line + " - " + e.getMessage());
                            }
                        }
                        // If only value is present
                        else if (parts.length == 1 && !parts[0].isEmpty()) {
                            try {
                                points.add(Long.parseLong(parts[0]));
                            } catch (NumberFormatException e) {
                                LOG.warning("Invalid number in line: " + line + " - " + e.getMessage());
                            }
                        }
                    }
                }

                LOG.info("Loaded " + points.size() + " partition points from " + path);

                // Verify integrity if checksum is present
                if (foundChecksum) {
                    long[] pointsArray = points.stream().mapToLong(Long::longValue).toArray();
                    long actualChecksum = calculateChecksum(pointsArray);

                    if (actualChecksum != expectedChecksum) {
                        LOG.warning("Checksum mismatch! Expected: " + expectedChecksum + ", Actual: " + actualChecksum);
                        points = null; // Mark for retry
                        retries++;
                        continue;
                    } else {
                        LOG.info("Checksum verified successfully: " + actualChecksum);
                    }
                }

            } catch (IOException e) {
                lastException = e;
                LOG.log(Level.WARNING, "Failed to load partition points (attempt " + (retries + 1) + "): " + e.getMessage(), e);
                retries++;

                if (retries < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * retries);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting to retry", ie);
                    }
                }
            }
        }

        if (points == null) {
            throw new IOException("Failed to load partition points after " + MAX_RETRIES + " attempts", lastException);
        }

        // Sort partition points in ascending order
        Collections.sort(points);

        // Remove duplicate points
        points = new ArrayList<>(new LinkedHashSet<>(points));

        long[] result = new long[points.size()];
        for (int i = 0; i < points.size(); i++) {
            result[i] = points.get(i);
        }

        return result;
    }

    /**
     * Converts to string representation
     */
    public static String toString(long[] points) {
        if (points == null || points.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(points[0]);

        for (int i = 1; i < points.length; i++) {
            sb.append(",").append(points[i]);
        }

        return sb.toString();
    }

    /**
     * Parses from string with enhanced error handling
     */
    public static long[] parse(String str) {
        if (str == null || str.trim().isEmpty()) {
            return new long[0];
        }

        String[] parts = str.split(",");
        List<Long> validPoints = new ArrayList<>(parts.length);

        for (String part : parts) {
            try {
                part = part.trim();
                if (!part.isEmpty()) {
                    validPoints.add(Long.parseLong(part));
                }
            } catch (NumberFormatException e) {
                LOG.warning("Invalid partition point value: " + part + " - " + e.getMessage());
            }
        }

        // Sort and remove duplicate points
        Collections.sort(validPoints);
        validPoints = new ArrayList<>(new LinkedHashSet<>(validPoints));

        // Convert to array
        long[] result = new long[validPoints.size()];
        for (int i = 0; i < validPoints.size(); i++) {
            result[i] = validPoints.get(i);
        }

        return result;
    }

    /**
     * Calculates simple checksum for array of points
     */
    private static long calculateChecksum(long[] points) {
        long checksum = 0;
        for (long point : points) {
            checksum = 31 * checksum + point;
        }
        return checksum;
    }
}
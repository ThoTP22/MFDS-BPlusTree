package com.hadoop.bplustree.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Quản lý thông tin phân vùng (Cải tiến)
 */
public class PartitionInfo {

    private static final Logger LOG = Logger.getLogger(PartitionInfo.class.getName());
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000;

    /**
     * Lưu thông tin phân vùng với cơ chế retry
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

                // Tạo thư mục cha nếu cần
                Path parent = path.getParent();
                if (parent != null && !fs.exists(parent)) {
                    fs.mkdirs(parent);
                }

                // Lưu với hỗ trợ kiểm tra tính toàn vẹn
                FSDataOutputStream out = fs.create(path);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

                try {
                    // Lưu header với thông tin số lượng điểm
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

                    // Thêm checksum đơn giản
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
     * Đọc thông tin phân vùng với cơ chế retry và kiểm tra tính toàn vẹn
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

                        // Bỏ qua dòng trống hoặc comment (trừ dòng checksum)
                        if (line.isEmpty() || (line.startsWith("#") && !line.startsWith("# CHECKSUM:"))) {
                            continue;
                        }

                        // Đọc checksum nếu có
                        if (line.startsWith("# CHECKSUM:")) {
                            expectedChecksum = Long.parseLong(line.substring("# CHECKSUM:".length()).trim());
                            foundChecksum = true;
                            continue;
                        }

                        // Xử lý dòng dữ liệu
                        String[] parts = line.split("\\s+");

                        // Nếu có format index\tvalue, lấy phần value
                        if (parts.length >= 2) {
                            try {
                                points.add(Long.parseLong(parts[1]));
                            } catch (NumberFormatException e) {
                                LOG.warning("Invalid format in line: " + line + " - " + e.getMessage());
                            }
                        }
                        // Nếu chỉ có value
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

                // Kiểm tra tính toàn vẹn nếu có checksum
                if (foundChecksum) {
                    long[] pointsArray = points.stream().mapToLong(Long::longValue).toArray();
                    long actualChecksum = calculateChecksum(pointsArray);

                    if (actualChecksum != expectedChecksum) {
                        LOG.warning("Checksum mismatch! Expected: " + expectedChecksum + ", Actual: " + actualChecksum);
                        points = null; // Đánh dấu để thử lại
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

        // Sắp xếp các điểm phân vùng theo thứ tự tăng dần
        Collections.sort(points);

        // Loại bỏ các điểm trùng lặp
        points = new ArrayList<>(new LinkedHashSet<>(points));

        long[] result = new long[points.size()];
        for (int i = 0; i < points.size(); i++) {
            result[i] = points.get(i);
        }

        return result;
    }

    /**
     * Chuyển đổi thành chuỗi
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
     * Phân tích từ chuỗi với xử lý lỗi tốt hơn
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

        // Sắp xếp và loại bỏ điểm trùng lặp
        Collections.sort(validPoints);
        validPoints = new ArrayList<>(new LinkedHashSet<>(validPoints));

        // Chuyển đổi thành mảng
        long[] result = new long[validPoints.size()];
        for (int i = 0; i < validPoints.size(); i++) {
            result[i] = validPoints.get(i);
        }

        return result;
    }

    /**
     * Tính checksum đơn giản cho mảng các điểm
     */
    private static long calculateChecksum(long[] points) {
        long checksum = 0;
        for (long point : points) {
            checksum = 31 * checksum + point;
        }
        return checksum;
    }
}
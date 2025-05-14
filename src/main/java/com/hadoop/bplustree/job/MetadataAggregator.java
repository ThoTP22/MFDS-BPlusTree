package com.hadoop.bplustree.job;

import com.hadoop.bplustree.partition.PartitionInfo;
import com.hadoop.bplustree.utils.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Phase 3: Tổng hợp metadata từ các cây B+Tree đã xây dựng (Cải tiến)
 */
public class MetadataAggregator extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MetadataAggregator.class.getName());
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000;
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MetadataAggregator <output_dir> <parts> <tree_order>");
            return 1;
        }

        String outputDir = args[0];
        int parts = Integer.parseInt(args[1]);
        int treeOrder = Integer.parseInt(args[2]);

        LOG.info("======= Phase 3: Metadata Aggregation =======");
        LOG.info("Output directory: " + outputDir);
        LOG.info("Number of partitions: " + parts);
        LOG.info("Tree order: " + treeOrder);
        LOG.info("Using " + Math.min(MAX_THREADS, parts) + " threads for parallel processing");
        LOG.info("===========================================");

        Timer.start("metadata-aggregation");

        try {
            // Kiểm tra sự tồn tại của các file cần thiết
            validateInputs(outputDir, parts);

            // Tạo thống kê và metadata (song song)
            createMetadataParallel(outputDir, parts, treeOrder);

            // Tạo metadata dạng JSON
            createJsonMetadata(outputDir, parts);

            // Tạo báo cáo tổng hợp
            createSummaryReport(outputDir, parts, treeOrder);

            long totalTime = Timer.end("metadata-aggregation");
            LOG.info("Metadata aggregation completed in " + (totalTime / 1000.0) + " seconds");

            return 0;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error in metadata aggregation: " + e.getMessage(), e);
            return 1;
        }
    }

    /**
     * Kiểm tra sự tồn tại của các file đầu vào
     */
    private void validateInputs(String outputDir, int parts) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Kiểm tra thư mục tree
        Path treeDirPath = new Path(outputDir + "/tree");
        if (!fs.exists(treeDirPath)) {
            throw new FileNotFoundException("Tree directory not found: " + treeDirPath);
        }

        // Kiểm tra file điểm phân vùng
        Path pointsPath = new Path(outputDir + "/points.txt");
        if (!fs.exists(pointsPath)) {
            LOG.warning("Partition points file not found: " + pointsPath);
        }

        // Kiểm tra các file tree
        int missingTrees = 0;
        for (int i = 0; i < parts; i++) {
            Path treePath = new Path(outputDir + "/tree/tree-" + i + ".dat");
            if (!fs.exists(treePath)) {
                LOG.warning("Tree file not found: " + treePath);
                missingTrees++;
            }
        }

        if (missingTrees > 0) {
            LOG.warning(missingTrees + " out of " + parts + " tree files are missing");
        }
    }

    /**
     * Tạo metadata dạng JSON cho tất cả các subtrees
     */
    private void createJsonMetadata(String outputDir, int parts) throws IOException, InterruptedException {
        LOG.info("Creating JSON metadata for " + parts + " subtrees");
        Timer.start("json-metadata");

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Đọc các điểm phân vùng
        long[] partitionPoints = null;
        try {
            Path pointsPath = new Path(outputDir + "/points.txt");
            if (fs.exists(pointsPath)) {
                partitionPoints = PartitionInfo.load(pointsPath, conf);
                LOG.info("Loaded " + partitionPoints.length + " partition points for JSON metadata");
            }
        } catch (Exception e) {
            LOG.warning("Could not load partition points: " + e.getMessage());
        }

        // Tạo cấu trúc JSON
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");
        jsonBuilder.append("  \"metadata\": {\n");
        jsonBuilder.append("    \"generated\": \"").append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date())).append("\",\n");
        jsonBuilder.append("    \"partitions\": ").append(parts).append(",\n");
        jsonBuilder.append("    \"version\": \"1.0\"\n");
        jsonBuilder.append("  },\n");
        jsonBuilder.append("  \"subtrees\": [\n");

        // Danh sách các task đọc thông tin cây
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(MAX_THREADS, parts));
        List<Callable<SubtreeInfo>> tasks = new ArrayList<>();

        for (int i = 0; i < parts; i++) {
            final int treeIndex = i;
            final long[] points = partitionPoints;

            tasks.add(() -> {
                SubtreeInfo info = new SubtreeInfo(treeIndex);

                // Thiết lập phạm vi giá trị
                if (points != null) {
                    if (treeIndex == 0) {
                        info.minKey = Long.MIN_VALUE;
                        info.maxKey = points[0];
                    } else if (treeIndex == parts - 1) {
                        info.minKey = points[treeIndex - 1];
                        info.maxKey = Long.MAX_VALUE;
                    } else {
                        info.minKey = points[treeIndex - 1];
                        info.maxKey = points[treeIndex];
                    }
                }

                // Đọc thông tin cây
                Path treePath = new Path(outputDir + "/tree/tree-" + treeIndex + ".dat");
                if (fs.exists(treePath)) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(fs.open(treePath)))) {

                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.startsWith("Height:")) {
                                info.height = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                            } else if (line.startsWith("Root type:")) {
                                info.rootType = line.substring(line.indexOf(":") + 1).trim();
                            } else if (line.startsWith("Number of records:")) {
                                info.records = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                            }
                        }
                        info.exists = true;
                    } catch (Exception e) {
                        LOG.warning("Error reading tree-" + treeIndex + ".dat: " + e.getMessage());
                    }
                }

                return info;
            });
        }

        try {
            // Thực thi tất cả các task và thu thập kết quả
            List<SubtreeInfo> treeInfos = new ArrayList<>();
            List<Future<SubtreeInfo>> futures = executor.invokeAll(tasks);

            for (Future<SubtreeInfo> future : futures) {
                try {
                    treeInfos.add(future.get());
                } catch (InterruptedException e) {
                    LOG.warning("Task interrupted: " + e.getMessage());
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.warning("Error executing task: " + e.getMessage());
                }
            }

            // Sắp xếp theo thứ tự index
            treeInfos.sort(Comparator.comparingInt(info -> info.index));

            // Tạo JSON cho mỗi cây
            for (int i = 0; i < treeInfos.size(); i++) {
                SubtreeInfo info = treeInfos.get(i);

                if (info.exists) {
                    jsonBuilder.append("    {\n");
                    jsonBuilder.append("      \"index\": ").append(info.index).append(",\n");
                    jsonBuilder.append("      \"path\": \"").append(outputDir).append("/tree/tree-").append(info.index).append(".dat\",\n");
                    jsonBuilder.append("      \"range\": {\n");
                    jsonBuilder.append("        \"min\": ").append(info.minKey).append(",\n");
                    jsonBuilder.append("        \"max\": ").append(info.maxKey).append("\n");
                    jsonBuilder.append("      },\n");
                    jsonBuilder.append("      \"height\": ").append(info.height).append(",\n");
                    jsonBuilder.append("      \"records\": ").append(info.records).append(",\n");
                    jsonBuilder.append("      \"rootType\": \"").append(info.rootType).append("\"\n");
                    jsonBuilder.append("    }");

                    if (i < treeInfos.size() - 1 && treeInfos.get(i + 1).exists) {
                        jsonBuilder.append(",");
                    }
                    jsonBuilder.append("\n");
                }
            }
        } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, "Thread interrupted during parallel metadata aggregation", e);
            Thread.currentThread().interrupt(); // Đặt lại trạng thái bị gián đoạn
        } finally {
            executor.shutdown();
        }

        jsonBuilder.append("  ]\n");
        jsonBuilder.append("}\n");

        // Lưu file JSON
        Path jsonPath = new Path(outputDir + "/subtrees.json");
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(jsonPath), StandardCharsets.UTF_8))) {
            writer.write(jsonBuilder.toString());
        }

        long jsonTime = Timer.end("json-metadata");
        LOG.info("Created JSON metadata file: " + jsonPath + " in " + (jsonTime / 1000.0) + " seconds");
    }

    /**
     * Lớp lưu trữ thông tin cây con
     */
    private static class SubtreeInfo {
        int index;
        boolean exists = false;
        int height = 0;
        String rootType = "";
        int records = 0;
        long minKey = Long.MIN_VALUE;
        long maxKey = Long.MAX_VALUE;

        public SubtreeInfo(int index) {
            this.index = index;
        }
    }

    /**
     * Tạo metadata tổng thể cho tất cả các cây song song
     */
    private void createMetadataParallel(String outputDir, int parts, int treeOrder) throws IOException, InterruptedException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Đường dẫn file thống kê
        Path statsPath = new Path(outputDir + "/tree_stats.txt");

        LOG.info("Gathering tree statistics in parallel");
        Timer.start("tree-stats");

        // Tạo thread pool để xử lý song song
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(MAX_THREADS, parts));
        List<Callable<TreeStats>> tasks = new ArrayList<>();

        // Tạo task cho mỗi cây
        for (int i = 0; i < parts; i++) {
            final int treeIndex = i;
            final Path treePath = new Path(outputDir + "/tree/tree-" + i + ".dat");

            tasks.add(() -> {
                TreeStats stats = new TreeStats(treeIndex);
                if (fs.exists(treePath)) {
                    stats.exists = true;

                    int retries = 0;
                    boolean success = false;

                    while (!success && retries < MAX_RETRIES) {
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(fs.open(treePath)))) {

                            String line;
                            while ((line = reader.readLine()) != null) {
                                if (line.startsWith("Height:")) {
                                    stats.height = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                                } else if (line.startsWith("Root type:")) {
                                    stats.rootType = line.substring(line.indexOf(":") + 1).trim();
                                } else if (line.startsWith("Number of records:")) {
                                    stats.records = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                                } else if (line.startsWith("Order:")) {
                                    stats.order = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                                } else if (line.startsWith("Building method:")) {
                                    stats.buildingMethod = line.substring(line.indexOf(":") + 1).trim();
                                }
                            }
                            success = true;
                        } catch (IOException e) {
                            LOG.warning("Error reading tree file (attempt " + (retries+1) + "): " + e.getMessage());
                            retries++;
                            if (retries < MAX_RETRIES) {
                                Thread.sleep(RETRY_DELAY_MS * retries);
                            }
                        }
                    }
                }
                return stats;
            });
        }

        // Thực thi tất cả các task song song
        List<TreeStats> allStats = new ArrayList<>();
        try {
            List<Future<TreeStats>> futures = executor.invokeAll(tasks);
            for (Future<TreeStats> future : futures) {
                try {
                    allStats.add(future.get());
                } catch (InterruptedException e) {
                    LOG.warning("Task interrupted: " + e.getMessage());
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.warning("Error executing task: " + e.getMessage());
                }
            }
        } finally {
            executor.shutdown();
        }

        // Sắp xếp theo thứ tự index
        allStats.sort(Comparator.comparingInt(stats -> stats.index));

        // Tạo file thống kê
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(statsPath)))) {

            writer.write("B+Tree Construction Statistics");
            writer.newLine();
            writer.write("Partitions: " + parts);
            writer.newLine();
            writer.write("Tree Order: " + treeOrder);
            writer.newLine();
            writer.write("Generated On: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            writer.newLine();
            writer.write("-------------------------------");
            writer.newLine();

            int totalRecords = 0;
            int maxHeight = 0;
            boolean allTreesExist = true;
            Set<String> buildingMethods = new HashSet<>();

            // Thêm thông tin từ mỗi cây
            for (TreeStats stats : allStats) {
                writer.write("Tree " + stats.index + ":");
                writer.newLine();

                if (stats.exists) {
                    writer.write("  Order: " + stats.order);
                    writer.newLine();
                    writer.write("  Height: " + stats.height);
                    writer.newLine();
                    writer.write("  Root type: " + stats.rootType);
                    writer.newLine();
                    writer.write("  Number of records: " + stats.records);
                    writer.newLine();
                    writer.write("  Building method: " + stats.buildingMethod);
                    writer.newLine();

                    totalRecords += stats.records;
                    maxHeight = Math.max(maxHeight, stats.height);
                    buildingMethods.add(stats.buildingMethod);

                    LOG.info(String.format("Tree %d: Height=%d, Root=%s, Records=%d",
                            stats.index, stats.height, stats.rootType, stats.records));
                } else {
                    writer.write("  NOT FOUND");
                    writer.newLine();
                    allTreesExist = false;
                    LOG.warning("Tree " + stats.index + " not found");
                }

                writer.write("-------------------------------");
                writer.newLine();
            }

            // Thống kê tổng hợp
            writer.write("Summary:");
            writer.newLine();
            writer.write("  Total records: " + totalRecords);
            writer.newLine();
            writer.write("  Maximum tree height: " + maxHeight);
            writer.newLine();
            writer.write("  All trees constructed: " + (allTreesExist ? "YES" : "NO"));
            writer.newLine();
            writer.write("  Building method(s): " + String.join(", ", buildingMethods));
            writer.newLine();
            writer.write("  Generated on: " + new java.util.Date());
            writer.newLine();

            LOG.info("Created tree statistics file: " + statsPath);
            LOG.info("Total records: " + totalRecords);
            LOG.info("Maximum tree height: " + maxHeight);
        }

        long statsTime = Timer.end("tree-stats");
        LOG.info("Tree statistics generated in " + (statsTime / 1000.0) + " seconds");

        // Tạo file index.html để dễ dàng xem kết quả
        createHTMLReport(outputDir, allStats, fs);
    }

    /**
     * Lớp lưu trữ thống kê cây
     */
    private static class TreeStats {
        int index;
        boolean exists = false;
        int height = 0;
        int order = 0;
        String rootType = "";
        int records = 0;
        String buildingMethod = "Unknown";

        public TreeStats(int index) {
            this.index = index;
        }
    }

    /**
     * Tạo báo cáo HTML cải tiến
     */
    private void createHTMLReport(String outputDir, List<TreeStats> allStats, FileSystem fs) throws IOException {
        Path htmlPath = new Path(outputDir + "/index.html");

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(htmlPath)))) {

            writer.write("<!DOCTYPE html>");
            writer.newLine();
            writer.write("<html lang=\"en\"><head><meta charset=\"UTF-8\">");
            writer.newLine();
            writer.write("<title>B+Tree Construction Report</title>");
            writer.newLine();
            writer.write("<style>");
            writer.write("body{font-family:Arial,sans-serif;margin:20px;line-height:1.6;color:#333;max-width:1200px;margin:0 auto;padding:20px}");
            writer.write("h1,h2{color:#2c3e50;border-bottom:1px solid #eee;padding-bottom:10px}");
            writer.write("th,td{padding:12px;text-align:left;border-bottom:1px solid #ddd}");
            writer.write("table{border-collapse:collapse;width:100%;margin-bottom:30px}");
            writer.write("thead{background-color:#f8f9fa}");
            writer.write("th{background-color:#4CAF50;color:white}");
            writer.write(".highlight{background-color:#f5f5f5}");
            writer.write(".container{background-color:white;box-shadow:0 0 10px rgba(0,0,0,0.1);padding:30px;border-radius:5px}");
            writer.write(".summary-box{background-color:#e8f5e9;padding:15px;border-radius:5px;margin-bottom:20px}");
            writer.write(".warning{color:#e53935}");
            writer.write(".chart-container{height:300px;margin-bottom:30px}");
            writer.write("</style>");
            writer.newLine();
            writer.write("</head><body>");
            writer.newLine();
            writer.write("<div class=\"container\">");
            writer.newLine();
            writer.write("<h1>B+Tree Construction Report</h1>");
            writer.newLine();
            writer.write("<p>Generated on: " + new java.util.Date() + "</p>");
            writer.newLine();

            // Thống kê tổng hợp
            int totalRecords = 0;
            int maxHeight = 0;
            int existingTrees = 0;

            for (TreeStats stats : allStats) {
                if (stats.exists) {
                    totalRecords += stats.records;
                    maxHeight = Math.max(maxHeight, stats.height);
                    existingTrees++;
                }
            }

            writer.write("<div class=\"summary-box\">");
            writer.write("<h2>Summary</h2>");
            writer.write("<p><strong>Total Records:</strong> " + totalRecords + "</p>");
            writer.write("<p><strong>Partitions:</strong> " + allStats.size() +
                    " (Found: " + existingTrees + ", Missing: " + (allStats.size() - existingTrees) + ")</p>");
            writer.write("<p><strong>Maximum Tree Height:</strong> " + maxHeight + "</p>");

            if (existingTrees < allStats.size()) {
                writer.write("<p class=\"warning\"><strong>Warning:</strong> Some trees are missing!</p>");
            }

            writer.write("</div>");
            writer.newLine();

            writer.write("<h2>Tree Information</h2>");
            writer.newLine();
            writer.write("<table>");
            writer.newLine();
            writer.write("<thead><tr><th>Partition</th><th>Height</th><th>Records</th><th>Root Type</th><th>Building Method</th><th>Status</th></tr></thead>");
            writer.newLine();
            writer.write("<tbody>");
            writer.newLine();

            for (TreeStats stats : allStats) {
                writer.write("<tr class=\"" + (stats.index % 2 == 0 ? "highlight" : "") + "\">");
                writer.write("<td>" + stats.index + "</td>");

                if (stats.exists) {
                    writer.write("<td>" + stats.height + "</td>");
                    writer.write("<td>" + stats.records + "</td>");
                    writer.write("<td>" + stats.rootType + "</td>");
                    writer.write("<td>" + stats.buildingMethod + "</td>");
                    writer.write("<td>OK</td>");
                } else {
                    writer.write("<td colspan=\"4\" class=\"warning\">NOT FOUND</td>");
                    writer.write("<td class=\"warning\">Missing</td>");
                }

                writer.write("</tr>");
                writer.newLine();
            }

            writer.write("</tbody></table>");
            writer.newLine();

            writer.write("<h2>Files</h2>");
            writer.newLine();
            writer.write("<ul>");
            writer.newLine();
            writer.write("<li><a href=\"tree_stats.txt\">Detailed Statistics</a></li>");
            writer.newLine();
            writer.write("<li><a href=\"points.txt\">Partition Points</a></li>");
            writer.newLine();
            writer.write("<li><a href=\"subtrees.json\">Subtrees JSON Metadata</a></li>");
            writer.newLine();
            writer.write("</ul>");
            writer.newLine();

            writer.write("</div>");
            writer.newLine();
            writer.write("</body></html>");
            writer.newLine();
        }

        LOG.info("Created HTML report: " + htmlPath);
    }

    /**
     * Tạo báo cáo tổng hợp
     */
    private void createSummaryReport(String outputDir, int parts, int treeOrder) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Đường dẫn file báo cáo
        Path reportPath = new Path(outputDir + "/summary_report.txt");

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(reportPath)))) {

            writer.write("=============================================");
            writer.newLine();
            writer.write("      B+TREE DISTRIBUTED CONSTRUCTION SUMMARY");
            writer.newLine();
            writer.write("=============================================");
            writer.newLine();
            writer.write("Generated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            writer.newLine();
            writer.write("Configuration:");
            writer.newLine();
            writer.write("  - Partitions: " + parts);
            writer.newLine();
            writer.write("  - Tree Order: " + treeOrder);
            writer.newLine();

            // Thu thập thông tin từ MapReduce counters
            // Trong thực tế, bạn sẽ đọc từ file counter hoặc job history

            // Kiểm tra kích thước của dữ liệu
            long totalSize = 0;
            try {
                Path dataPath = new Path(outputDir + "/data");
                if (fs.exists(dataPath)) {
                    FileStatus[] statuses = fs.listStatus(dataPath);
                    for (FileStatus status : statuses) {
                        totalSize += status.getLen();
                    }
                }
            } catch (Exception e) {
                LOG.warning("Error getting data size: " + e.getMessage());
            }

            writer.write("Data:");
            writer.newLine();
            writer.write("  - Total size: " + (totalSize / (1024 * 1024)) + " MB");
            writer.newLine();

            // Kiểm tra thời gian thực thi từ Timer
            writer.write("Execution Time:");
            writer.newLine();

            long phase1Time = Timer.get("phase1-job");
            long phase2Time = Timer.get("data-distribution-job");
            long phase3Time = Timer.get("metadata-aggregation");

            writer.write(String.format("  - Phase 1 (Partition Finding): %.2f seconds", phase1Time / 1000.0));
            writer.newLine();
            writer.write(String.format("  - Phase 2 (Data Distribution): %.2f seconds", phase2Time / 1000.0));
            writer.newLine();
            writer.write(String.format("  - Phase 3 (Metadata Aggregation): %.2f seconds", phase3Time / 1000.0));
            writer.newLine();

            long totalTime = phase1Time + phase2Time + phase3Time;
            writer.write(String.format("  - Total: %.2f seconds", totalTime / 1000.0));
            writer.newLine();

            writer.write("=============================================");
            writer.newLine();
        }

        LOG.info("Created summary report: " + reportPath);
    }
}
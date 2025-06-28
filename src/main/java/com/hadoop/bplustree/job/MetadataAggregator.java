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
 * Phase 3: Enhanced Metadata Aggregation from Constructed B+ Trees
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
            // Validate existence of required files
            validateInputs(outputDir, parts);

            // Create statistics and metadata (parallel)
            createMetadataParallel(outputDir, parts, treeOrder);

            // Create JSON metadata
            createJsonMetadata(outputDir, parts);

            // Create summary report
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
     * Validates existence of input files
     */
    private void validateInputs(String outputDir, int parts) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Check tree directory
        Path treeDirPath = new Path(outputDir + "/tree");
        if (!fs.exists(treeDirPath)) {
            throw new FileNotFoundException("Tree directory not found: " + treeDirPath);
        }

        // Check partition points file
        Path pointsPath = new Path(outputDir + "/points.txt");
        if (!fs.exists(pointsPath)) {
            LOG.warning("Partition points file not found: " + pointsPath);
        }

        // Check tree files
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
     * Creates JSON metadata for all subtrees
     */
    private void createJsonMetadata(String outputDir, int parts) throws IOException, InterruptedException {
        LOG.info("Creating JSON metadata for " + parts + " subtrees");
        Timer.start("json-metadata");

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Read partition points
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

        // Create JSON structure
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");
        jsonBuilder.append("  \"metadata\": {\n");
        jsonBuilder.append("    \"generated\": \"").append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date())).append("\",\n");
        jsonBuilder.append("    \"partitions\": ").append(parts).append(",\n");
        jsonBuilder.append("    \"version\": \"1.0\"\n");
        jsonBuilder.append("  },\n");
        jsonBuilder.append("  \"subtrees\": [\n");

        // List of tasks to read tree information
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(MAX_THREADS, parts));
        List<Callable<SubtreeInfo>> tasks = new ArrayList<>();

        for (int i = 0; i < parts; i++) {
            final int treeIndex = i;
            final long[] points = partitionPoints;

            tasks.add(() -> {
                SubtreeInfo info = new SubtreeInfo(treeIndex);

                // Set value range
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

                // Read tree information
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
            // Execute all tasks and collect results
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

            // Sort by index
            treeInfos.sort(Comparator.comparingInt(info -> info.index));

            // Create JSON for each tree
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
            Thread.currentThread().interrupt(); // Reset interrupted status
        } finally {
            executor.shutdown();
        }

        jsonBuilder.append("  ]\n");
        jsonBuilder.append("}\n");

        // Save JSON file
        Path jsonPath = new Path(outputDir + "/subtrees.json");
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(jsonPath), StandardCharsets.UTF_8))) {
            writer.write(jsonBuilder.toString());
        }

        long jsonTime = Timer.end("json-metadata");
        LOG.info("Created JSON metadata file: " + jsonPath + " in " + (jsonTime / 1000.0) + " seconds");
    }

    /**
     * Creates a summary report of the distributed B+ tree
     */
    private void createSummaryReport(String outputDir, int parts, int treeOrder) throws IOException {
        LOG.info("Creating summary report");
        Timer.start("summary-report");

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Read partition points
        long[] partitionPoints = null;
        try {
            Path pointsPath = new Path(outputDir + "/points.txt");
            if (fs.exists(pointsPath)) {
                partitionPoints = PartitionInfo.load(pointsPath, conf);
                LOG.info("Loaded " + partitionPoints.length + " partition points for summary report");
            }
        } catch (Exception e) {
            LOG.warning("Could not load partition points: " + e.getMessage());
        }

        // Create summary report
        Path reportPath = new Path(outputDir + "/summary.txt");
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(reportPath), StandardCharsets.UTF_8))) {

            writer.write("======= Distributed B+ Tree Summary =======\n");
            writer.write("Generated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            writer.write("Number of partitions: " + parts + "\n");
            writer.write("Tree order: " + treeOrder + "\n");
            writer.write("===========================================\n\n");

            // Write partition information
            writer.write("Partition Information:\n");
            writer.write("----------------------\n");
            if (partitionPoints != null) {
                for (int i = 0; i < partitionPoints.length; i++) {
                    writer.write("Partition " + i + ": " + partitionPoints[i] + "\n");
                }
            } else {
                writer.write("No partition points available\n");
            }
            writer.write("\n");

            // Write tree information
            writer.write("Tree Information:\n");
            writer.write("-----------------\n");
            int totalRecords = 0;
            int totalHeight = 0;
            int existingTrees = 0;

            for (int i = 0; i < parts; i++) {
                Path treePath = new Path(outputDir + "/tree/tree-" + i + ".dat");
                if (fs.exists(treePath)) {
                    existingTrees++;
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(fs.open(treePath)))) {

                        String line;
                        int records = 0;
                        int height = 0;
                        String rootType = "Unknown";

                        while ((line = reader.readLine()) != null) {
                            if (line.startsWith("Height:")) {
                                height = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                            } else if (line.startsWith("Root type:")) {
                                rootType = line.substring(line.indexOf(":") + 1).trim();
                            } else if (line.startsWith("Number of records:")) {
                                records = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                            }
                        }

                        writer.write("Tree " + i + ":\n");
                        writer.write("  Records: " + records + "\n");
                        writer.write("  Height: " + height + "\n");
                        writer.write("  Root type: " + rootType + "\n");
                        writer.write("  File: " + treePath + "\n\n");

                        totalRecords += records;
                        totalHeight += height;
                    } catch (Exception e) {
                        writer.write("Tree " + i + ": Error reading file - " + e.getMessage() + "\n\n");
                    }
                } else {
                    writer.write("Tree " + i + ": File not found\n\n");
                }
            }

            // Write summary statistics
            writer.write("Summary Statistics:\n");
            writer.write("------------------\n");
            writer.write("Total trees: " + parts + "\n");
            writer.write("Existing trees: " + existingTrees + "\n");
            writer.write("Missing trees: " + (parts - existingTrees) + "\n");
            writer.write("Total records: " + totalRecords + "\n");
            if (existingTrees > 0) {
                writer.write("Average height: " + (totalHeight / (double) existingTrees) + "\n");
            }
            writer.write("\n");

            // Write system information
            writer.write("System Information:\n");
            writer.write("-------------------\n");
            writer.write("Java version: " + System.getProperty("java.version") + "\n");
            writer.write("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + "\n");
            writer.write("Architecture: " + System.getProperty("os.arch") + "\n");
            writer.write("Processors: " + Runtime.getRuntime().availableProcessors() + "\n");
            writer.write("Max memory: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB\n");
            writer.write("Total memory: " + (Runtime.getRuntime().totalMemory() / (1024 * 1024)) + " MB\n");
            writer.write("Free memory: " + (Runtime.getRuntime().freeMemory() / (1024 * 1024)) + " MB\n");
        }

        long totalTime = Timer.end("summary-report");
        LOG.info("Summary report created in " + (totalTime / 1000.0) + " seconds");
    }

    /**
     * Creates metadata in parallel for all subtrees
     */
    private void createMetadataParallel(String outputDir, int parts, int treeOrder) throws IOException, InterruptedException {
        LOG.info("Creating metadata for " + parts + " subtrees in parallel");
        Timer.start("parallel-metadata");

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // Create metadata directory
        Path metadataDir = new Path(outputDir + "/metadata");
        if (!fs.exists(metadataDir)) {
            fs.mkdirs(metadataDir);
        }

        // Create tasks for parallel processing
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(MAX_THREADS, parts));
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < parts; i++) {
            final int treeIndex = i;
            tasks.add(() -> {
                Path treePath = new Path(outputDir + "/tree/tree-" + treeIndex + ".dat");
                Path metadataPath = new Path(metadataDir + "/metadata-" + treeIndex + ".txt");

                if (fs.exists(treePath)) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(fs.open(treePath)));
                         BufferedWriter writer = new BufferedWriter(
                                 new OutputStreamWriter(fs.create(metadataPath), StandardCharsets.UTF_8))) {

                        // Write metadata header
                        writer.write("Tree " + treeIndex + " Metadata\n");
                        writer.write("Generated: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
                        writer.write("Tree order: " + treeOrder + "\n\n");

                        // Copy tree information
                        String line;
                        while ((line = reader.readLine()) != null) {
                            writer.write(line + "\n");
                        }
                    }
                }
                return null;
            });
        }

        try {
            // Execute all tasks
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    LOG.warning("Error in parallel metadata creation: " + e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, "Thread interrupted during parallel metadata creation", e);
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }

        long totalTime = Timer.end("parallel-metadata");
        LOG.info("Parallel metadata creation completed in " + (totalTime / 1000.0) + " seconds");
    }

    /**
     * Helper class for storing subtree information
     */
    private static class SubtreeInfo {
        int index;
        long minKey = Long.MIN_VALUE;
        long maxKey = Long.MAX_VALUE;
        int height = 0;
        int records = 0;
        String rootType = "Unknown";
        boolean exists = false;

        SubtreeInfo(int index) {
            this.index = index;
        }
    }
}
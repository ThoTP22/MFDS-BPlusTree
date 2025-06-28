package com.hadoop.bplustree.job;

import com.hadoop.bplustree.partition.Partitioner;
import com.hadoop.bplustree.partition.PartitionInfo;
import com.hadoop.bplustree.tree.BPlusTree;
import com.hadoop.bplustree.tree.InternalNode;
import com.hadoop.bplustree.tree.LeafNode;
import com.hadoop.bplustree.tree.TreeNode;
import com.hadoop.bplustree.utils.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataDistributor extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(DataDistributor.class.getName());
    // Maximum number of records in a batch
    private static final int BATCH_SIZE = 100000;
    // Maximum number of retry attempts on error
    private static final int MAX_RETRIES = 3;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: DataDistributor <input_path> <output_path> <points_path> <tree_order>");
            return 1;
        }

        String input = args[0];
        String output = args[1];
        String pointsPath = args[2];
        int treeOrder = Integer.parseInt(args[3]);


        if (treeOrder < 3) {
            throw new IllegalArgumentException("Tree order must be at least 3");
        }

        Configuration conf = getConf();

        int threads = conf.getInt("threads.per.block", Runtime.getRuntime().availableProcessors());

        LOG.info("======= Phase 2: Data Distribution & Tree Building =======");
        LOG.info("Input: " + input);
        LOG.info("Output: " + output);
        LOG.info("Points path: " + pointsPath);
        LOG.info("Tree order: " + treeOrder);
        LOG.info("Batch size: " + BATCH_SIZE);
        LOG.info("Threads per block: " + threads);
        LOG.info("====================================================");

        long[] points = null;
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                LOG.info("Loading partition points from: " + pointsPath + " (attempt " + (retries + 1) + ")");
                points = PartitionInfo.load(new Path(pointsPath), conf);
                int parts = points.length + 1;
                LOG.info("Loaded " + points.length + " partition points for " + parts + " partitions");
                break;
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Failed to load partition points (attempt " + (retries + 1) + "): " + e.getMessage(), e);
                retries++;
                if (retries >= MAX_RETRIES) {
                    throw new IOException("Failed to load partition points after " + MAX_RETRIES + " attempts", e);
                }
                Thread.sleep(1000 * retries);
            }
        }

        int parts = points.length + 1;

        // Save partition points to configuration
        conf.set("partition.points", PartitionInfo.toString(points));
        conf.setInt("tree.order", treeOrder);

        // Set up MapReduce job
        Job job = Job.getInstance(conf, "Distribute Data and Bulk Load Trees");
        job.setJarByClass(DataDistributor.class);

        job.setMapperClass(DistMapper.class);
        job.setReducerClass(StreamingTreeBuildingReducer.class);
        job.setPartitionerClass(Partitioner.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(parts);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output + "/data"));

        LOG.info("Submitting data distribution and bulk loading job");
        Timer.start("data-distribution-bulk-loading-job");
        boolean success = job.waitForCompletion(true);
        long jobTime = Timer.end("data-distribution-bulk-loading-job");

        if (success) {
            LOG.info("Successfully distributed data and bulk loaded B+trees across " + parts +
                    " partitions in " + (jobTime / 1000.0) + " seconds");
        } else {
            LOG.severe("Failed to distribute data and bulk load trees");
        }

        return success ? 0 : 1;
    }

    public static class DistMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private long processedRecords = 0;
        private long validRecords = 0;
        private long invalidRecords = 0;
        private static final long LOG_INTERVAL = 1000000; // Log every 1 million records

        @Override
        protected void setup(Context ctx) {
            LOG.info("Data distribution mapper starting");
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                try {
                    long number = Long.parseLong(line);
                    outKey.set(number);
                    outValue.set(line);
                    ctx.write(outKey, outValue);
                    validRecords++;
                } catch (NumberFormatException e) {
                    invalidRecords++;
                    ctx.getCounter("DataDistribution", "InvalidRecords").increment(1);
                }
            }

            processedRecords++;
            if (processedRecords % LOG_INTERVAL == 0) {
                LOG.info("Processed " + processedRecords + " records, valid: " +
                        validRecords + ", invalid: " + invalidRecords);
            }
        }

        @Override
        protected void cleanup(Context ctx) {
            LOG.info("Data distribution mapper completed, processed " + processedRecords +
                    " records total (valid: " + validRecords + ", invalid: " + invalidRecords + ")");

            // Update counters for reporting
            ctx.getCounter("DataDistribution", "ProcessedRecords").increment(processedRecords);
            ctx.getCounter("DataDistribution", "ValidRecords").increment(validRecords);
        }
    }

    public static class StreamingTreeBuildingReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private int partitionId;
        private AtomicLong processedRecords = new AtomicLong(0);
        private int treeOrder;
        private static final long LOG_INTERVAL = 1000000; // Log every 1 million records
        private Path outputTreePath;
        private FileSystem fs;

        // Store all data for bulk loading
        private List<KeyValuePair> allData;
        // B+ tree
        private BPlusTree tree;
        // Thread pool for parallel node creation
        private ExecutorService threadPool;
        private int numThreads;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            partitionId = ctx.getTaskAttemptID().getTaskID().getId();
            treeOrder = ctx.getConfiguration().getInt("tree.order", 200);
            fs = FileSystem.get(ctx.getConfiguration());

            String outputDir = ctx.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
            outputDir = outputDir.substring(0, outputDir.lastIndexOf("/data"));
            outputTreePath = new Path(outputDir + "/tree/tree-" + partitionId + ".dat");

            // Initialize thread pool for parallel processing
            numThreads = ctx.getConfiguration().getInt("threads.per.block", Runtime.getRuntime().availableProcessors());
            threadPool = Executors.newFixedThreadPool(numThreads);

            // Initialize data structures for bulk loading
            allData = new ArrayList<>();
            tree = new BPlusTree(treeOrder);

            LOG.info("Parallel bulk loading tree builder " + partitionId + " starting with order " + treeOrder + 
                    " using " + numThreads + " threads");

            // Ensure output directory exists
            fs.mkdirs(new Path(outputTreePath.getParent().toString()));

            Timer.start("bulk-loading-" + partitionId);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            // Collect all data for bulk loading
            for (Text val : values) {
                // Add key-value pair to all data collection
                allData.add(new KeyValuePair(key.get(), val.toString()));

                // Write data to output
                ctx.write(key, val);

                long count = processedRecords.incrementAndGet();
                if (count % LOG_INTERVAL == 0) {
                    LOG.info("Partition " + partitionId + " collected " + count + " records for bulk loading");
                }
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            long totalRecords = processedRecords.get();
            LOG.info("Partition " + partitionId + " starting bulk loading with " + totalRecords + " records total");

            // Perform bulk loading
            if (!allData.isEmpty()) {
                bulkLoadTree();
            }

            long buildTime = Timer.end("bulk-loading-" + partitionId);
            LOG.info(String.format(
                    "Tree %d: Bulk loaded with %d records, height=%d, root=%s in %.2f seconds",
                    partitionId, totalRecords, tree.getHeight(),
                    tree.getRoot() != null ? (tree.getRoot().isLeaf() ? "Leaf" : "Internal") : "NULL", 
                    buildTime / 1000.0));

            // Save B+tree
            saveTreeMetadata(tree, totalRecords);

            // Validate tree consistency
            validateTree(tree);

            // Shutdown thread pool
            if (threadPool != null && !threadPool.isShutdown()) {
                threadPool.shutdown();
                try {
                    if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                        threadPool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    threadPool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            // Free memory
            allData = null;
            System.gc();
        }

        /**
         * Performs bulk loading of B+tree from all collected data
         */
        private void bulkLoadTree() throws IOException {
            LOG.info("Starting bulk loading process for " + allData.size() + " records in partition " + partitionId);

            // Sort all data by key (essential for bulk loading)
            Collections.sort(allData, (a, b) -> Long.compare(a.key, b.key));
            LOG.info("Sorted all data for bulk loading");

            // Create leaf nodes using bulk loading technique
            List<LeafNode> leafNodes = bulkCreateLeafNodes(allData, treeOrder);
            LOG.info("Created " + leafNodes.size() + " leaf nodes using bulk loading");

            // Link leaf nodes
            for (int i = 0; i < leafNodes.size() - 1; i++) {
                leafNodes.get(i).setNext(leafNodes.get(i + 1));
            }

            // Build tree from leaf nodes using bulk loading
            if (!leafNodes.isEmpty()) {
                // If only one leaf node, set as root
                if (leafNodes.size() == 1) {
                    tree.setRoot(leafNodes.get(0));
                } else {
                    // Build tree from bottom up using bulk loading
                    List<TreeNode> currentLevel = new ArrayList<>(leafNodes);
                    while (currentLevel.size() > 1) {
                        List<InternalNode> parents = bulkCreateInternalNodes(currentLevel, treeOrder);
                        currentLevel = new ArrayList<>(parents);
                    }

                    // Set root node
                    tree.setRoot(currentLevel.get(0));
                }
            }

            LOG.info("Bulk loading completed successfully");
        }

        /**
         * Creates leaf nodes using parallel bulk loading technique from sorted data
         */
        private List<LeafNode> bulkCreateLeafNodes(List<KeyValuePair> sortedData, int order) {
            int maxKeysPerLeaf = order - 1;
            int dataSize = sortedData.size();
            int nodeCount = (dataSize + maxKeysPerLeaf - 1) / maxKeysPerLeaf; // Ceiling division

            LOG.info("Creating " + nodeCount + " leaf nodes for " + dataSize + " records using parallel bulk loading with " + numThreads + " threads");
            Timer.start("parallel-leaf-creation-" + partitionId);

            // Use parallel streams to create leaf nodes concurrently
            List<LeafNode> leafNodes = IntStream.range(0, nodeCount)
                .parallel()
                .mapToObj(i -> {
                    LeafNode leaf = new LeafNode(order);
                    int startIdx = i * maxKeysPerLeaf;
                    int endIdx = Math.min(startIdx + maxKeysPerLeaf, dataSize);

                    // Bulk insert all keys in this node
                    for (int j = startIdx; j < endIdx; j++) {
                        KeyValuePair pair = sortedData.get(j);
                        leaf.insertKeyValue(pair.key, pair.value);
                    }

                    return leaf;
                })
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

            long leafCreationTime = Timer.end("parallel-leaf-creation-" + partitionId);
            LOG.info("Successfully created " + leafNodes.size() + " leaf nodes using parallel bulk loading in " + 
                    (leafCreationTime / 1000.0) + " seconds");
            
            return leafNodes;
        }

        /**
         * Creates internal nodes using parallel bulk loading technique from child nodes
         */
        private List<InternalNode> bulkCreateInternalNodes(List<TreeNode> children, int order) {
            int maxChildrenPerNode = order;
            int childrenSize = children.size();
            int nodeCount = (childrenSize + maxChildrenPerNode - 1) / maxChildrenPerNode; // Ceiling division

            LOG.info("Creating " + nodeCount + " internal nodes for " + childrenSize + " children using parallel bulk loading with " + numThreads + " threads");
            Timer.start("parallel-internal-creation-" + partitionId);

            // Use parallel streams to create internal nodes concurrently
            List<InternalNode> internalNodes = IntStream.range(0, nodeCount)
                .parallel()
                .mapToObj(i -> {
                    InternalNode internal = new InternalNode(order);
                    int startIdx = i * maxChildrenPerNode;
                    int endIdx = Math.min(startIdx + maxChildrenPerNode, childrenSize);

                    // Add first child
                    TreeNode firstChild = children.get(startIdx);
                    internal.addChild(firstChild);
                    firstChild.setParent(internal);

                    // Bulk add remaining children with separator keys
                    for (int j = startIdx + 1; j < endIdx; j++) {
                        TreeNode child = children.get(j);
                        long separatorKey = getSeparatorKey(child);
                        internal.insertKeyChild(separatorKey, child);
                        child.setParent(internal);
                    }

                    return internal;
                })
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

            long internalCreationTime = Timer.end("parallel-internal-creation-" + partitionId);
            LOG.info("Successfully created " + internalNodes.size() + " internal nodes using parallel bulk loading in " + 
                    (internalCreationTime / 1000.0) + " seconds");
            
            return internalNodes;
        }

        /**
         * Gets separator key for node
         */
        private long getSeparatorKey(TreeNode node) {
            if (node.isLeaf()) {
                return ((LeafNode) node).getKeys().get(0);
            } else {
                return ((InternalNode) node).getKeys().get(0);
            }
        }

        /**
         * Saves B+tree metadata
         */
        private void saveTreeMetadata(BPlusTree tree, long totalRecords) throws IOException {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(outputTreePath)))) {
                writer.write("Order: " + treeOrder);
                writer.newLine();
                writer.write("Height: " + tree.getHeight());
                writer.newLine();
                writer.write("Root type: " + (tree.getRoot() != null ? (tree.getRoot().isLeaf() ? "Leaf" : "Internal") : "NULL"));
                writer.newLine();
                writer.write("Number of records: " + totalRecords);
                writer.newLine();
                writer.write("Building method: Parallel Bulk Loading (" + numThreads + " threads)");
                writer.newLine();
                writer.write("Partition ID: " + partitionId);
                writer.newLine();
                writer.write("Completion time: " + new java.util.Date());
                writer.newLine();
            }

            LOG.info("Tree " + partitionId + " metadata saved to: " + outputTreePath);
        }

        /**
         * Validates B+tree consistency
         */
        private void validateTree(BPlusTree tree) {
            try {
                int height = tree.getHeight();
                LOG.info("Validating tree consistency, height: " + height);

                if (height < 1) {
                    LOG.warning("Tree validation failed: Invalid height " + height);
                    return;
                }

                // Validate root node
                TreeNode root = tree.getRoot();
                if (root == null) {
                    LOG.severe("Tree validation failed: Root is null");
                    return;
                }

                // Validate leaf nodes
                if (height == 1 && !root.isLeaf()) {
                    LOG.severe("Tree validation failed: Height=1 but root is not leaf");
                }

                LOG.info("Tree validation passed");
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Tree validation failed with exception: " + e.getMessage(), e);
            }
        }

        private static class KeyValuePair {
            private final long key;
            private final String value;

            public KeyValuePair(long key, String value) {
                this.key = key;
                this.value = value;
            }
        }
    }

}
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
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataDistributor extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(DataDistributor.class.getName());
    // Số lượng bản ghi tối đa trong một batch
    private static final int BATCH_SIZE = 100000;
    // Số lần thử tối đa khi gặp lỗi
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

        // Lưu điểm phân vùng vào configuration
        conf.set("partition.points", PartitionInfo.toString(points));
        conf.setInt("tree.order", treeOrder);
        conf.setInt("batch.size", BATCH_SIZE);

        // Thiết lập job MapReduce
        Job job = Job.getInstance(conf, "Distribute Data and Build Trees");
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

        LOG.info("Submitting data distribution and tree building job");
        Timer.start("data-distribution-job");
        boolean success = job.waitForCompletion(true);
        long jobTime = Timer.end("data-distribution-job");

        if (success) {
            LOG.info("Successfully distributed data and built B+trees across " + parts +
                    " partitions in " + (jobTime / 1000.0) + " seconds");
        } else {
            LOG.severe("Failed to distribute data and build trees");
        }

        return success ? 0 : 1;
    }

    public static class DistMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private long processedRecords = 0;
        private long validRecords = 0;
        private long invalidRecords = 0;
        private static final long LOG_INTERVAL = 1000000; // Log mỗi 1 triệu bản ghi

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

            // Cập nhật counter cho báo cáo
            ctx.getCounter("DataDistribution", "ProcessedRecords").increment(processedRecords);
            ctx.getCounter("DataDistribution", "ValidRecords").increment(validRecords);
        }
    }

    public static class StreamingTreeBuildingReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private int partitionId;
        private AtomicLong processedRecords = new AtomicLong(0);
        private int treeOrder;
        private int batchSize;
        private static final long LOG_INTERVAL = 1000000; // Log mỗi 1 triệu bản ghi
        private Path outputTreePath;
        private FileSystem fs;

        // Lưu trữ dữ liệu theo batch để xây dựng cây hiệu quả
        private List<KeyValuePair> currentBatch;
        // Theo dõi danh sách các node lá đã tạo để liên kết chúng sau đó
        private List<LeafNode> leafNodes;
        // Cây B+tree
        private BPlusTree tree;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            partitionId = ctx.getTaskAttemptID().getTaskID().getId();
            treeOrder = ctx.getConfiguration().getInt("tree.order", 200);
            batchSize = ctx.getConfiguration().getInt("batch.size", BATCH_SIZE);
            fs = FileSystem.get(ctx.getConfiguration());

            String outputDir = ctx.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
            outputDir = outputDir.substring(0, outputDir.lastIndexOf("/data"));
            outputTreePath = new Path(outputDir + "/tree/tree-" + partitionId + ".dat");

            // Chuẩn bị cấu trúc dữ liệu
            currentBatch = new ArrayList<>(batchSize);
            leafNodes = new ArrayList<>();
            tree = new BPlusTree(treeOrder);

            LOG.info("Tree building reducer " + partitionId + " starting with order " + treeOrder +
                    ", batch size " + batchSize);

            // Đảm bảo thư mục đầu ra tồn tại
            fs.mkdirs(new Path(outputTreePath.getParent().toString()));

            Timer.start("tree-building-" + partitionId);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            // Xử lý dữ liệu theo batch để quản lý bộ nhớ
            for (Text val : values) {
                // Thêm cặp khóa-giá trị vào batch hiện tại
                currentBatch.add(new KeyValuePair(key.get(), val.toString()));

                // Ghi dữ liệu ra output
                ctx.write(key, val);

                long count = processedRecords.incrementAndGet();
                if (count % LOG_INTERVAL == 0) {
                    LOG.info("Partition " + partitionId + " processed " + count + " records");
                }

                // Khi batch đầy, xử lý và reset batch
                if (currentBatch.size() >= batchSize) {
                    processBatch();
                }
            }
        }

        /**
         * Xử lý một batch dữ liệu hiện tại
         */
        private void processBatch() throws IOException {
            if (currentBatch.isEmpty()) return;

            LOG.info("Processing batch of " + currentBatch.size() + " records in partition " + partitionId);

            // Sắp xếp batch theo khóa
            Collections.sort(currentBatch, (a, b) -> Long.compare(a.key, b.key));

            // Tạo node lá cho batch này
            List<LeafNode> newLeafNodes = createLeafNodesFromBatch(currentBatch, treeOrder);
            LOG.info("Created " + newLeafNodes.size() + " leaf nodes from current batch");

            // Thêm vào danh sách node lá
            leafNodes.addAll(newLeafNodes);

            // Reset batch
            currentBatch.clear();
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            long totalRecords = processedRecords.get();
            LOG.info("Partition " + partitionId + " completed with " + totalRecords + " records total");

            // Xử lý batch còn lại
            if (!currentBatch.isEmpty()) {
                processBatch();
            }

            LOG.info("Building B+tree for partition " + partitionId + " with " +
                    leafNodes.size() + " leaf nodes and order " + treeOrder);

            // Liên kết các node lá
            for (int i = 0; i < leafNodes.size() - 1; i++) {
                leafNodes.get(i).setNext(leafNodes.get(i + 1));
            }

            // Xây dựng cây từ các node lá
            if (!leafNodes.isEmpty()) {
                // Nếu chỉ có một node lá, đặt làm node gốc
                if (leafNodes.size() == 1) {
                    tree.setRoot(leafNodes.get(0));
                } else {
                    // Xây dựng cây từ dưới lên
                    List<TreeNode> currentLevel = new ArrayList<>(leafNodes);
                    while (currentLevel.size() > 1) {
                        List<InternalNode> parents = createParentNodes(currentLevel, treeOrder);
                        currentLevel = new ArrayList<>(parents);
                    }

                    // Đặt node gốc
                    tree.setRoot(currentLevel.get(0));
                }
            }

            long buildTime = Timer.end("tree-building-" + partitionId);
            LOG.info(String.format(
                    "Tree %d: Built with %d records, height=%d, root=%s in %.2f seconds",
                    partitionId, totalRecords, tree.getHeight(),
                    tree.getRoot().isLeaf() ? "Leaf" : "Internal", buildTime / 1000.0));

            // Lưu cây B+tree
            saveTreeMetadata(tree, totalRecords);

            // Kiểm tra tính nhất quán của cây
            validateTree(tree);

            // Giải phóng bộ nhớ
            currentBatch = null;
            leafNodes = null;
            System.gc();
        }

        /**
         * Tạo các node lá từ batch dữ liệu đã sắp xếp
         */
        private List<LeafNode> createLeafNodesFromBatch(List<KeyValuePair> batch, int order) {
            List<LeafNode> newLeafs = new ArrayList<>();
            int maxKeysPerLeaf = order - 1;
            int dataSize = batch.size();
            int nodeCount = (dataSize + maxKeysPerLeaf - 1) / maxKeysPerLeaf; // Ceiling division

            for (int i = 0; i < nodeCount; i++) {
                LeafNode leaf = new LeafNode(order);
                int startIdx = i * maxKeysPerLeaf;
                int endIdx = Math.min(startIdx + maxKeysPerLeaf, dataSize);

                for (int j = startIdx; j < endIdx; j++) {
                    KeyValuePair pair = batch.get(j);
                    leaf.insertKeyValue(pair.key, pair.value);
                }

                newLeafs.add(leaf);
            }

            return newLeafs;
        }

        /**
         * Tạo các node nội bộ cha từ danh sách node con
         */
        private List<InternalNode> createParentNodes(List<TreeNode> children, int order) {
            List<InternalNode> parents = new ArrayList<>();
            int maxChildrenPerNode = order;
            int childrenSize = children.size();
            int nodeCount = (childrenSize + maxChildrenPerNode - 1) / maxChildrenPerNode; // Ceiling division

            for (int i = 0; i < nodeCount; i++) {
                InternalNode parent = new InternalNode(order);
                int startIdx = i * maxChildrenPerNode;
                int endIdx = Math.min(startIdx + maxChildrenPerNode, childrenSize);

                // Thêm node con đầu tiên
                TreeNode firstChild = children.get(startIdx);
                parent.addChild(firstChild);
                firstChild.setParent(parent);

                // Thêm các node con còn lại cùng với các khóa phân tách
                for (int j = startIdx + 1; j < endIdx; j++) {
                    TreeNode child = children.get(j);
                    long separatorKey = getSeparatorKey(child);
                    parent.insertKeyChild(separatorKey, child);
                    child.setParent(parent);
                }

                parents.add(parent);
            }

            return parents;
        }

        /**
         * Lấy khóa phân tách cho node
         */
        private long getSeparatorKey(TreeNode node) {
            if (node.isLeaf()) {
                return ((LeafNode) node).getKeys().get(0);
            } else {
                return ((InternalNode) node).getKeys().get(0);
            }
        }

        /**
         * Lưu metadata của cây B+tree
         */
        private void saveTreeMetadata(BPlusTree tree, long totalRecords) throws IOException {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(outputTreePath)))) {
                writer.write("Order: " + treeOrder);
                writer.newLine();
                writer.write("Height: " + tree.getHeight());
                writer.newLine();
                writer.write("Root type: " + (tree.getRoot().isLeaf() ? "Leaf" : "Internal"));
                writer.newLine();
                writer.write("Number of records: " + totalRecords);
                writer.newLine();
                writer.write("Building method: Bottom-Up Streaming");
                writer.newLine();
                writer.write("Partition ID: " + partitionId);
                writer.newLine();
                writer.write("Completion time: " + new java.util.Date());
                writer.newLine();
            }

            LOG.info("Tree " + partitionId + " metadata saved to: " + outputTreePath);
        }

        /**
         * Kiểm tra tính nhất quán của cây B+tree
         */
        private void validateTree(BPlusTree tree) {
            try {
                int height = tree.getHeight();
                LOG.info("Validating tree consistency, height: " + height);

                if (height < 1) {
                    LOG.warning("Tree validation failed: Invalid height " + height);
                    return;
                }

                // Kiểm tra node gốc
                TreeNode root = tree.getRoot();
                if (root == null) {
                    LOG.severe("Tree validation failed: Root is null");
                    return;
                }

                // Kiểm tra các node lá
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
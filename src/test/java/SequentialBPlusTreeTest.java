import com.hadoop.bplustree.tree.BPlusTree;
import com.hadoop.bplustree.tree.InternalNode;
import com.hadoop.bplustree.tree.LeafNode;
import com.hadoop.bplustree.tree.TreeNode;
import com.hadoop.bplustree.utils.Timer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Test class for sequential B+Tree construction using two methods:
 * 1. Insertion (Top-down): Insert each element into the tree
 * 2. Bottom-up: Build from leaf nodes upward
 *
 * This class reads data from a CSV file containing Long numbers to build a B+Tree
 */
public class SequentialBPlusTreeTest {

    private static final Logger LOG = Logger.getLogger(SequentialBPlusTreeTest.class.getName());
    private static final String REPORT_FILE = "bplustree_test_report.txt";

    // Hardcoded parameters - modify these values as needed
    private static final String CSV_FILE = "dataset/2gb.csv"; // Path to your CSV file
    private static final int TREE_ORDER = 500;// Order of B+Tree
    private static final String METHOD = "top-down"; // Construoyction method: "top-down", "bottom-up", or "both"

    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        // Use hardcoded parameters instead of command line arguments
        String csvFile = CSV_FILE;
        int treeOrder = TREE_ORDER;
        String method = METHOD.toLowerCase();

        LOG.info("===== B+Tree Sequential Construction Test =====");
        LOG.info("CSV File: " + csvFile);
        LOG.info("Tree order: " + treeOrder);
        LOG.info("Method: " + method);
        LOG.info("==============================================");

        // Read data from CSV file
        long[] data = readDataFromCSV(csvFile);

        // Perform tests based on the chosen method
        if (method.equals("top-down") || method.equals("both")) {
            testTopDownConstruction(data, treeOrder);
        }

        if (method.equals("bottom-up") || method.equals("both")) {
            testBottomUpConstruction(data, treeOrder);
        }

        // Compare both methods if both were run
        if (method.equals("both")) {
            compareConstructionMethods();
        }

        // Print timing report
        Timer.report();
        try {
            Timer.save(REPORT_FILE);
            LOG.info("Timing report saved to: " + REPORT_FILE);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Could not save timing report: " + e.getMessage(), e);
        }
    }

    /**
     * Compare both construction methods
     */
    private static void compareConstructionMethods() {
        LOG.info("Comparing construction methods...");

        long topDownTime = Timer.getTime("top-down-construction");
        long bottomUpTime = Timer.getTime("bottom-up-construction");
        long topDownSearchTime = Timer.getTime("top-down-search-test");
        long bottomUpSearchTime = Timer.getTime("bottom-up-search-test");

        String fasterConstruction = topDownTime <= bottomUpTime ? "Top-down" : "Bottom-up";
        String fasterSearch = topDownSearchTime <= bottomUpSearchTime ? "Top-down" : "Bottom-up";

        LOG.info("Construction time comparison:");
        LOG.info("- Top-down: " + (topDownTime / 1000.0) + " seconds");
        LOG.info("- Bottom-up: " + (bottomUpTime / 1000.0) + " seconds");
        LOG.info("- Faster method: " + fasterConstruction + " (by " +
                Math.abs(topDownTime - bottomUpTime) / 1000.0 + " seconds)");

        LOG.info("Search time comparison:");
        LOG.info("- Top-down: " + (topDownSearchTime / 1000.0) + " seconds");
        LOG.info("- Bottom-up: " + (bottomUpSearchTime / 1000.0) + " seconds");
        LOG.info("- Faster method: " + fasterSearch + " (by " +
                Math.abs(topDownSearchTime - bottomUpSearchTime) / 1000.0 + " seconds)");

        // Save comparison report
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("construction_comparison.txt"))) {
            writer.write("B+Tree Construction Methods Comparison");
            writer.newLine();
            writer.write("======================================");
            writer.newLine();

            writer.write("Construction time:");
            writer.newLine();
            writer.write("- Top-down: " + (topDownTime / 1000.0) + " seconds");
            writer.newLine();
            writer.write("- Bottom-up: " + (bottomUpTime / 1000.0) + " seconds");
            writer.newLine();
            writer.write("- Faster method: " + fasterConstruction + " (by " +
                    Math.abs(topDownTime - bottomUpTime) / 1000.0 + " seconds)");
            writer.newLine();

            writer.write("Search time:");
            writer.newLine();
            writer.write("- Top-down: " + (topDownSearchTime / 1000.0) + " seconds");
            writer.newLine();
            writer.write("- Bottom-up: " + (bottomUpSearchTime / 1000.0) + " seconds");
            writer.newLine();
            writer.write("- Faster method: " + fasterSearch + " (by " +
                    Math.abs(topDownSearchTime - bottomUpSearchTime) / 1000.0 + " seconds)");
            writer.newLine();

            writer.write("======================================");
            writer.newLine();
            writer.write("Generated on: " + new java.util.Date());

            LOG.info("Comparison report saved to: construction_comparison.txt");
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Could not save comparison report: " + e.getMessage(), e);
        }
    }


    private static long[] readDataFromCSV(String csvFile) {
        LOG.info("Reading data from CSV file: " + csvFile);
        Timer.start("data-reading");

        List<Long> dataList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            int lineCount = 0;
            int validCount = 0;
            int invalidCount = 0;

            while ((line = reader.readLine()) != null) {
                lineCount++;
                // Skip header line if present
                if (lineCount == 1 && !isNumeric(line.trim())) {
                    LOG.info("Skipping header line: " + line);
                    continue;
                }

                // Process data line
                String[] values = line.split(",");
                for (String value : values) {
                    try {
                        String trimmed = value.trim();
                        if (!trimmed.isEmpty()) {
                            long number = Long.parseLong(trimmed);
                            dataList.add(number);
                            validCount++;
                        }
                    } catch (NumberFormatException e) {
                        invalidCount++;
                    }
                }

                // Log progress periodically
                if (lineCount % 100000 == 0) {
                    LOG.info("Processed " + lineCount + " lines, valid numbers: " + validCount);
                }
            }

            LOG.info("Finished reading CSV file. Total lines: " + lineCount);
            LOG.info("Valid numbers found: " + validCount);
            LOG.info("Invalid entries skipped: " + invalidCount);

        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Error reading CSV file: " + e.getMessage(), e);
            System.exit(1);
        }

        // Convert List to array
        long[] data = new long[dataList.size()];
        for (int i = 0; i < dataList.size(); i++) {
            data[i] = dataList.get(i);
        }

        long readingTime = Timer.end("data-reading");
        LOG.info("Data reading completed in " + (readingTime / 1000.0) + " seconds");
        LOG.info("Total data points: " + data.length);

        return data;
    }

    /**
     * Check if a string is numeric
     */
    private static boolean isNumeric(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Test top-down (insertion) construction
     */
    private static void testTopDownConstruction(long[] data, int order) {
        LOG.info("Starting Top-down (insertion) construction with " + data.length + " elements...");
        Timer.start("top-down-construction");

        // Initialize B+Tree
        BPlusTree tree = new BPlusTree(order);

        // Insert each element into the tree
        for (int i = 0; i < data.length; i++) {
            tree.insert(data[i], String.valueOf(data[i]));

            // Log progress
            if ((i + 1) % 100000 == 0 || i == data.length - 1) {
                LOG.info("Top-down: Inserted " + (i + 1) + "/" + data.length + " elements");
            }
        }

        long constructionTime = Timer.end("top-down-construction");
        LOG.info("Top-down construction completed in " + (constructionTime / 1000.0) + " seconds");

        // Check tree consistency
        boolean isValid = tree.validateTree();
        LOG.info("Tree validation: " + (isValid ? "PASSED" : "FAILED"));

        // Statistics
        LOG.info("Tree height: " + tree.getHeight());
        LOG.info("Total nodes: " + tree.getNodeCount());
        LOG.info("Cache stats: " + tree.getCacheStatistics());

        // Test search performance
        testSearchPerformance(tree, data, "top-down");

        // Save report
        saveTreeReport(tree, "top-down", constructionTime, data.length, true);
    }

    /**
     * Test bottom-up construction
     */
    private static void testBottomUpConstruction(long[] data, int order) {
        LOG.info("Starting Bottom-up construction with " + data.length + " elements...");
        Timer.start("bottom-up-construction");

        // Sort data first
        Timer.start("bottom-up-sorting");
        List<KeyValuePair> sortedData = new ArrayList<>(data.length);
        for (long value : data) {
            sortedData.add(new KeyValuePair(value, String.valueOf(value)));
        }
        Collections.sort(sortedData);
        Timer.end("bottom-up-sorting");

        // Build leaf nodes
        LOG.info("Creating leaf nodes...");
        List<LeafNode> leafNodes = createLeafNodes(sortedData, order);
        LOG.info("Created " + leafNodes.size() + " leaf nodes");

        // Link leaf nodes
        for (int i = 0; i < leafNodes.size() - 1; i++) {
            leafNodes.get(i).setNext(leafNodes.get(i + 1));
        }

        // Build tree from leaf nodes upward
        BPlusTree tree = new BPlusTree(order);

        if (leafNodes.isEmpty()) {
            LOG.warning("No leaf nodes created!");
        } else if (leafNodes.size() == 1) {
            // If only one leaf node, set as root
            tree.setRoot(leafNodes.get(0));
        } else {
            // Build tree from bottom up
            List<TreeNode> currentLevel = new ArrayList<>(leafNodes);

            while (currentLevel.size() > 1) {
                List<InternalNode> parents = createParentNodes(currentLevel, order);
                LOG.info("Created " + parents.size() + " internal nodes at next level");
                currentLevel = new ArrayList<>(parents);
            }

            // Set root node
            tree.setRoot(currentLevel.get(0));
        }

        long constructionTime = Timer.end("bottom-up-construction");
        LOG.info("Bottom-up construction completed in " + (constructionTime / 1000.0) + " seconds");

        // Check tree consistency
        boolean isValid = tree.validateTree();
        LOG.info("Tree validation: " + (isValid ? "PASSED" : "FAILED"));

        // Statistics
        LOG.info("Tree height: " + tree.getHeight());
        LOG.info("Total nodes: " + tree.getNodeCount());

        // Test search performance
        testSearchPerformance(tree, data, "bottom-up");

        // Save report
        saveTreeReport(tree, "bottom-up", constructionTime, data.length, false);
    }

    /**
     * Create leaf nodes from sorted data
     */
    private static List<LeafNode> createLeafNodes(List<KeyValuePair> sortedData, int order) {
        List<LeafNode> leafNodes = new ArrayList<>();
        int maxKeysPerLeaf = order - 1;
        int dataSize = sortedData.size();
        int nodeCount = (dataSize + maxKeysPerLeaf - 1) / maxKeysPerLeaf; // Ceiling division

        for (int i = 0; i < nodeCount; i++) {
            LeafNode leaf = new LeafNode(order);
            int startIdx = i * maxKeysPerLeaf;
            int endIdx = Math.min(startIdx + maxKeysPerLeaf, dataSize);

            for (int j = startIdx; j < endIdx; j++) {
                KeyValuePair pair = sortedData.get(j);
                leaf.insertKeyValue(pair.key, pair.value);
            }

            leafNodes.add(leaf);

            // Log progress
            if ((i + 1) % 1000 == 0 || i == nodeCount - 1) {
                LOG.info("Created " + (i + 1) + "/" + nodeCount + " leaf nodes");
            }
        }

        return leafNodes;
    }

    /**
     * Create parent nodes from list of children
     */
    private static List<InternalNode> createParentNodes(List<TreeNode> children, int order) {
        List<InternalNode> parents = new ArrayList<>();
        int maxChildrenPerNode = order;
        int childrenSize = children.size();
        int nodeCount = (childrenSize + maxChildrenPerNode - 1) / maxChildrenPerNode; // Ceiling division

        for (int i = 0; i < nodeCount; i++) {
            InternalNode parent = new InternalNode(order);
            int startIdx = i * maxChildrenPerNode;
            int endIdx = Math.min(startIdx + maxChildrenPerNode, childrenSize);

            // Add first child
            TreeNode firstChild = children.get(startIdx);
            parent.addChild(firstChild);
            firstChild.setParent(parent);

            // Add remaining children with separator keys
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
     * Get separator key for a node
     */
    private static long getSeparatorKey(TreeNode node) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            return leaf.getKeys().get(0);
        } else {
            InternalNode internal = (InternalNode) node;
            return internal.getKeys().get(0);
        }
    }

    /**
     * Test search performance
     */
    private static void testSearchPerformance(BPlusTree tree, long[] data, String method) {
        LOG.info("Testing search performance for " + method + "...");
        Timer.start(method + "-search-test");

        // Randomly select elements to search
        Random random = new Random();
        int numSearches = Math.min(1000, data.length);
        int hits = 0;

        for (int i = 0; i < numSearches; i++) {
            long key = data[random.nextInt(data.length)];
            String value = tree.search(key);

            if (value != null) {
                hits++;
            }
        }

        long searchTime = Timer.end(method + "-search-test");
        double avgSearchTime = searchTime / (double) numSearches;

        LOG.info("Search test completed in " + (searchTime / 1000.0) + " seconds");
        LOG.info("Average search time: " + avgSearchTime + " ms");
        LOG.info("Hit rate: " + ((double) hits / numSearches * 100) + "%");
    }

    /**
     * Save B+Tree report
     */
    private static void saveTreeReport(BPlusTree tree, String method, long constructionTime, int dataSize, boolean includeCache) {
        String reportFile = "tree_" + method + "_report.txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportFile))) {
            writer.write("B+Tree Construction Report - " + method.toUpperCase());
            writer.newLine();
            writer.write("======================================");
            writer.newLine();
            writer.write("Data size: " + dataSize);
            writer.newLine();
            writer.write("Tree order: " + tree.getOrder());
            writer.newLine();
            writer.write("Construction time: " + (constructionTime / 1000.0) + " seconds");
            writer.newLine();
            writer.write("Construction method: " + method);
            writer.newLine();
            writer.write("Tree height: " + tree.getHeight());
            writer.newLine();
            writer.write("Total nodes: " + tree.getNodeCount());
            writer.newLine();
            writer.write("Root type: " + (tree.getRoot().isLeaf() ? "Leaf" : "Internal"));
            writer.newLine();
            writer.write("Tree validation: " + (tree.validateTree() ? "PASSED" : "FAILED"));
            writer.newLine();

            // Add cache stats for top-down method
            if (includeCache) {
                writer.write("Cache statistics: " + tree.getCacheStatistics());
                writer.newLine();
            }

            // Add search time info
            long searchTime = Timer.getTime(method + "-search-test");
            int numSearches = Math.min(1000, dataSize);
            double avgSearchTime = searchTime / (double) numSearches;

            writer.write("Search performance:");
            writer.newLine();
            writer.write("  - Total search time: " + (searchTime / 1000.0) + " seconds");
            writer.newLine();
            writer.write("  - Average search time: " + avgSearchTime + " ms");
            writer.newLine();

            writer.write("======================================");
            writer.newLine();
            writer.write("Generated on: " + new java.util.Date());

            LOG.info("Tree report saved to: " + reportFile);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Could not save tree report: " + e.getMessage(), e);
        }
    }

    /**
     * Key-Value pair class
     */
    private static class KeyValuePair implements Comparable<KeyValuePair> {
        private final long key;
        private final String value;

        public KeyValuePair(long key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(KeyValuePair other) {
            return Long.compare(this.key, other.key);
        }
    }
}
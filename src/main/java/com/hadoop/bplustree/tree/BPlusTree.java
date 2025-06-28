package com.hadoop.bplustree.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Enhanced B+ Tree Data Structure Implementation
 */
public class BPlusTree implements Serializable {

    private static final Logger LOG = Logger.getLogger(BPlusTree.class.getName());
    private static final long serialVersionUID = 1L;

    private TreeNode root;
    private final int order;
    private int nodeCount = 0;
    private int height = 1;

    // Cache for frequently accessed nodes (Simple LRU cache implementation)
    private transient Map<Long, LeafNode> nodeCache;
    private transient int cacheSize = 100;
    private transient List<Long> cacheOrder;

    // Performance metrics
    private transient long searches = 0;
    private transient long cacheHits = 0;

    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("Order must be at least 3");
        }

        this.order = order;
        this.root = new LeafNode(order);
        this.nodeCount = 1;
        initializeCache();
    }

    /**
     * Initializes the cache to improve search performance
     */
    private void initializeCache() {
        nodeCache = new HashMap<>(cacheSize);
        cacheOrder = new ArrayList<>(cacheSize);
    }

    /**
     * Sets the cache size
     */
    public void setCacheSize(int size) {
        if (size > 0) {
            this.cacheSize = size;
            initializeCache();
        }
    }

    /**
     * Sets the root node (for bottom-up construction)
     */
    public void setRoot(TreeNode root) {
        this.root = root;
        updateHeight();
        updateNodeCount();
    }

    /**
     * Updates the tree height
     */
    private void updateHeight() {
        int h = 0;
        TreeNode current = root;

        while (!current.isLeaf()) {
            h++;
            InternalNode internal = (InternalNode) current;
            if (internal.getChildren().isEmpty()) break;
            current = internal.getChildren().get(0);
        }

        this.height = h + 1;
    }

    /**
     * Updates the total node count
     */
    private void updateNodeCount() {
        this.nodeCount = countNodes(root);
    }

    /**
     * Counts the total number of nodes in the tree
     */
    private int countNodes(TreeNode node) {
        if (node == null) return 0;

        if (node.isLeaf()) {
            return 1;
        } else {
            InternalNode internal = (InternalNode) node;
            int count = 1;
            for (TreeNode child : internal.getChildren()) {
                count += countNodes(child);
            }
            return count;
        }
    }

    /**
     * Adds an entry to the cache
     */
    private void addToCache(long key, LeafNode node) {
        if (nodeCache == null) {
            initializeCache();
        }

        // Remove the oldest entry if the cache is full
        if (cacheOrder.size() >= cacheSize && !cacheOrder.isEmpty()) {
            Long oldestKey = cacheOrder.remove(0);
            nodeCache.remove(oldestKey);
        }

        // Add to cache
        nodeCache.put(key, node);
        cacheOrder.add(key);
    }

    /**
     * Inserts a key-value pair into the B+ tree
     */
    public void insert(long key, String value) {
        LeafNode leaf = findLeafNode(key);
        leaf.insertKeyValue(key, value);

        // Remove from cache as the node may have changed
        if (nodeCache != null) {
            nodeCache.remove(key);
        }

        if (leaf.getKeys().size() >= order) {
            splitLeafNode(leaf);
            updateHeight();
            updateNodeCount();
        }
    }

    /**
     * Searches for a value associated with a given key
     */
    public String search(long key) {
        searches++;

        // Check cache first
        if (nodeCache != null && nodeCache.containsKey(key)) {
            cacheHits++;
            LeafNode cachedNode = nodeCache.get(key);

            // Update cache order
            cacheOrder.remove(Long.valueOf(key));
            cacheOrder.add(key);

            return cachedNode.findValue(key);
        }

        // Perform normal search if not found in cache
        LeafNode leaf = findLeafNode(key);
        String value = leaf.findValue(key);

        // Add to cache if found
        if (value != null && nodeCache != null) {
            addToCache(key, leaf);
        }

        return value;
    }

    /**
     * Searches for all key-value pairs within a specified range
     */
    public List<KeyValuePair> searchRange(long startKey, long endKey) {
        List<KeyValuePair> result = new ArrayList<>();
        LeafNode leaf = findLeafNode(startKey);

        boolean done = false;
        while (leaf != null && !done) {
            List<Long> keys = leaf.getKeys();
            List<String> values = leaf.getValues();

            for (int i = 0; i < keys.size(); i++) {
                long key = keys.get(i);

                if (key >= startKey && key <= endKey) {
                    result.add(new KeyValuePair(key, values.get(i)));
                }

                if (key > endKey) {
                    done = true;
                    break;
                }
            }

            if (!done) {
                leaf = leaf.getNext();
            }
        }

        return result;
    }

    /**
     * Finds the leaf node containing the specified key with performance optimization
     */
    private LeafNode findLeafNode(long key) {
        TreeNode current = root;

        while (!current.isLeaf()) {
            InternalNode internal = (InternalNode) current;
            current = internal.findChild(key);
        }

        return (LeafNode) current;
    }

    /**
     * Splits a leaf node when it exceeds capacity
     */
    private void splitLeafNode(LeafNode leaf) {
        LeafNode newLeaf = leaf.split();
        nodeCount++;

        long newKey = newLeaf.getKeys().get(0);

        // Remove cache entries for split nodes
        if (nodeCache != null) {
            for (Long key : leaf.getKeys()) {
                nodeCache.remove(key);
            }
            for (Long key : newLeaf.getKeys()) {
                nodeCache.remove(key);
            }
        }

        if (leaf.getParent() == null) {
            InternalNode newRoot = new InternalNode(order);
            root = newRoot;
            nodeCount++;
            height++;

            newRoot.addChild(leaf);
            newRoot.insertKeyChild(newKey, newLeaf);
        } else {
            InternalNode parent = (InternalNode) leaf.getParent();
            parent.insertKeyChild(newKey, newLeaf);

            if (parent.getKeys().size() >= order) {
                splitInternalNode(parent);
            }
        }
    }

    /**
     * Splits an internal node when it exceeds capacity
     */
    private void splitInternalNode(InternalNode node) {
        long midKey = node.getMidKey();
        InternalNode newNode = node.split();
        nodeCount++;

        if (node.getParent() == null) {
            InternalNode newRoot = new InternalNode(order);
            root = newRoot;
            nodeCount++;
            height++;

            newRoot.addChild(node);
            newRoot.insertKeyChild(midKey, newNode);
        } else {
            InternalNode parent = (InternalNode) node.getParent();
            parent.insertKeyChild(midKey, newNode);

            if (parent.getKeys().size() >= order) {
                splitInternalNode(parent);
            }
        }
    }

    /**
     * Validates the structural integrity of the tree
     */
    public boolean validateTree() {
        if (root == null) {
            LOG.warning("Validation failed: Root is null");
            return false;
        }

        // Check height
        int calculatedHeight = calculateHeight();
        if (calculatedHeight != height) {
            LOG.warning("Validation failed: Height mismatch. Stored: " + height +
                    ", Calculated: " + calculatedHeight);
            return false;
        }

        // Check node count
        int calculatedNodeCount = countNodes(root);
        if (calculatedNodeCount != nodeCount) {
            LOG.warning("Validation failed: Node count mismatch. Stored: " + nodeCount +
                    ", Calculated: " + calculatedNodeCount);
            return false;
        }

        // Check consistency of each internal node
        return validateNode(root, null, null);
    }

    /**
     * Validates the consistency of a node and its subtree
     */
    private boolean validateNode(TreeNode node, Long minKey, Long maxKey) {
        if (node == null) return true;

        List<Long> keys = node.getKeys();

        // Check keys are in ascending order
        for (int i = 1; i < keys.size(); i++) {
            if (keys.get(i - 1) >= keys.get(i)) {
                LOG.warning("Validation failed: Keys not in ascending order: " +
                        keys.get(i - 1) + " >= " + keys.get(i));
                return false;
            }
        }

        // Check range
        if (minKey != null && !keys.isEmpty() && keys.get(0) < minKey) {
            LOG.warning("Validation failed: Key smaller than minimum allowed: " +
                    keys.get(0) + " < " + minKey);
            return false;
        }

        if (maxKey != null && !keys.isEmpty() && keys.get(keys.size() - 1) > maxKey) {
            LOG.warning("Validation failed: Key larger than maximum allowed: " +
                    keys.get(keys.size() - 1) + " > " + maxKey);
            return false;
        }

        // If internal node, check all children
        if (!node.isLeaf()) {
            InternalNode internal = (InternalNode) node;
            List<TreeNode> children = internal.getChildren();

            // Number of children must be correct
            if (children.size() != keys.size() + 1) {
                LOG.warning("Validation failed: Internal node with " + keys.size() +
                        " keys must have " + (keys.size() + 1) + " children, but has " +
                        children.size());
                return false;
            }

            // Check each child node and parent-child relationship
            for (int i = 0; i < children.size(); i++) {
                TreeNode child = children.get(i);

                // Check parent-child relationship
                if (child.getParent() != internal) {
                    LOG.warning("Validation failed: Parent-child relationship mismatch");
                    return false;
                }

                // Determine range for child node
                Long childMin = (i == 0) ? minKey : keys.get(i - 1);
                Long childMax = (i == children.size() - 1) ? maxKey : keys.get(i);

                // Recursively check child node
                if (!validateNode(child, childMin, childMax)) {
                    return false;
                }
            }
        } else {
            // Check linkage between leaf nodes
            LeafNode leaf = (LeafNode) node;
            LeafNode next = leaf.getNext();

            if (next != null && !keys.isEmpty() && !next.getKeys().isEmpty()) {
                if (keys.get(keys.size() - 1) >= next.getKeys().get(0)) {
                    LOG.warning("Validation failed: Leaf node sequence is invalid");
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Calculates the actual height of the tree
     */
    private int calculateHeight() {
        int h = 0;
        TreeNode current = root;

        while (!current.isLeaf()) {
            h++;
            InternalNode internal = (InternalNode) current;
            if (internal.getChildren().isEmpty()) break;
            current = internal.getChildren().get(0);
        }

        return h + 1;
    }

    /**
     * Outputs cache performance statistics
     */
    public String getCacheStatistics() {
        if (nodeCache == null) return "Cache not initialized";

        double hitRate = (searches > 0) ? (double) cacheHits / searches * 100 : 0;

        return String.format("Cache Statistics: Size=%d, Searches=%d, Hits=%d, Hit Rate=%.2f%%",
                nodeCache.size(), searches, cacheHits, hitRate);
    }

    public TreeNode getRoot() {
        return root;
    }

    public int getHeight() {
        return height;
    }

    public int getOrder() {
        return order;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    /**
     * Class representing a key-value pair
     */
    public static class KeyValuePair implements Serializable {
        private static final long serialVersionUID = 2L;
        private final long key;
        private final String value;

        public KeyValuePair(long key, String value) {
            this.key = key;
            this.value = value;
        }

        public long getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }
    }
}
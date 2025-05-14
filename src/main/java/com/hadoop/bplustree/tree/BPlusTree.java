package com.hadoop.bplustree.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Cấu trúc dữ liệu B+ tree (Cải tiến)
 */
public class BPlusTree implements Serializable {

    private static final Logger LOG = Logger.getLogger(BPlusTree.class.getName());
    private static final long serialVersionUID = 1L;

    private TreeNode root;
    private final int order;
    private int nodeCount = 0;
    private int height = 1;

    // Cache cho các node thường truy cập (LRU cache đơn giản)
    private transient Map<Long, LeafNode> nodeCache;
    private transient int cacheSize = 100;
    private transient List<Long> cacheOrder;

    // Thống kê hiệu suất
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
     * Khởi tạo cache để cải thiện hiệu suất tìm kiếm
     */
    private void initializeCache() {
        nodeCache = new HashMap<>(cacheSize);
        cacheOrder = new ArrayList<>(cacheSize);
    }

    /**
     * Đặt kích thước cache
     */
    public void setCacheSize(int size) {
        if (size > 0) {
            this.cacheSize = size;
            initializeCache();
        }
    }

    /**
     * Đặt node gốc (phục vụ cho việc xây dựng bottom-up)
     */
    public void setRoot(TreeNode root) {
        this.root = root;
        updateHeight();
        updateNodeCount();
    }

    /**
     * Cập nhật chiều cao cây
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
     * Cập nhật số lượng node
     */
    private void updateNodeCount() {
        this.nodeCount = countNodes(root);
    }

    /**
     * Đếm tổng số node trong cây
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
     * Thêm một mục vào cache
     */
    private void addToCache(long key, LeafNode node) {
        if (nodeCache == null) {
            initializeCache();
        }

        // Xóa đối tượng cũ nếu cache đầy
        if (cacheOrder.size() >= cacheSize && !cacheOrder.isEmpty()) {
            Long oldestKey = cacheOrder.remove(0);
            nodeCache.remove(oldestKey);
        }

        // Thêm vào cache
        nodeCache.put(key, node);
        cacheOrder.add(key);
    }

    /**
     * Chèn một cặp khóa-giá trị vào B+ tree
     */
    public void insert(long key, String value) {
        LeafNode leaf = findLeafNode(key);
        leaf.insertKeyValue(key, value);

        // Xóa khỏi cache vì node có thể đã thay đổi
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
     * Tìm giá trị cho một khóa
     */
    public String search(long key) {
        searches++;

        // Kiểm tra cache trước
        if (nodeCache != null && nodeCache.containsKey(key)) {
            cacheHits++;
            LeafNode cachedNode = nodeCache.get(key);

            // Cập nhật thứ tự cache
            cacheOrder.remove(Long.valueOf(key));
            cacheOrder.add(key);

            return cachedNode.findValue(key);
        }

        // Tìm kiếm bình thường nếu không có trong cache
        LeafNode leaf = findLeafNode(key);
        String value = leaf.findValue(key);

        // Thêm vào cache nếu tìm thấy
        if (value != null && nodeCache != null) {
            addToCache(key, leaf);
        }

        return value;
    }

    /**
     * Tìm tất cả cặp khóa-giá trị trong một khoảng
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
     * Tìm node lá chứa khóa với cải tiến hiệu suất
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
     * Chia node lá
     */
    private void splitLeafNode(LeafNode leaf) {
        LeafNode newLeaf = leaf.split();
        nodeCount++;

        long newKey = newLeaf.getKeys().get(0);

        // Xóa bỏ các mục cache cho node bị chia
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
     * Chia node nội bộ
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
     * Kiểm tra sự nhất quán của cây
     */
    public boolean validateTree() {
        if (root == null) {
            LOG.warning("Validation failed: Root is null");
            return false;
        }

        // Kiểm tra chiều cao
        int calculatedHeight = calculateHeight();
        if (calculatedHeight != height) {
            LOG.warning("Validation failed: Height mismatch. Stored: " + height +
                    ", Calculated: " + calculatedHeight);
            return false;
        }

        // Kiểm tra số lượng node
        int calculatedNodeCount = countNodes(root);
        if (calculatedNodeCount != nodeCount) {
            LOG.warning("Validation failed: Node count mismatch. Stored: " + nodeCount +
                    ", Calculated: " + calculatedNodeCount);
            return false;
        }

        // Kiểm tra tính nhất quán của mỗi nút nội bộ
        return validateNode(root, null, null);
    }

    /**
     * Kiểm tra tính nhất quán của một node và cây con của nó
     */
    private boolean validateNode(TreeNode node, Long minKey, Long maxKey) {
        if (node == null) return true;

        List<Long> keys = node.getKeys();

        // Kiểm tra keys theo thứ tự tăng dần
        for (int i = 1; i < keys.size(); i++) {
            if (keys.get(i - 1) >= keys.get(i)) {
                LOG.warning("Validation failed: Keys not in ascending order: " +
                        keys.get(i - 1) + " >= " + keys.get(i));
                return false;
            }
        }

        // Kiểm tra phạm vi
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

        // Nếu là node nội bộ, kiểm tra tất cả các node con
        if (!node.isLeaf()) {
            InternalNode internal = (InternalNode) node;
            List<TreeNode> children = internal.getChildren();

            // Số lượng con phải đúng
            if (children.size() != keys.size() + 1) {
                LOG.warning("Validation failed: Internal node with " + keys.size() +
                        " keys must have " + (keys.size() + 1) + " children, but has " +
                        children.size());
                return false;
            }

            // Kiểm tra mỗi node con và mối quan hệ cha-con
            for (int i = 0; i < children.size(); i++) {
                TreeNode child = children.get(i);

                // Kiểm tra mối quan hệ cha-con
                if (child.getParent() != internal) {
                    LOG.warning("Validation failed: Parent-child relationship mismatch");
                    return false;
                }

                // Xác định phạm vi cho node con
                Long childMin = (i == 0) ? minKey : keys.get(i - 1);
                Long childMax = (i == children.size() - 1) ? maxKey : keys.get(i);

                // Kiểm tra đệ quy trên node con
                if (!validateNode(child, childMin, childMax)) {
                    return false;
                }
            }
        } else {
            // Kiểm tra liên kết giữa các node lá
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
     * Tính chiều cao thực tế của cây
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
     * In thống kê hiệu suất cache
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
     * Lớp đại diện cho cặp khóa-giá trị
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
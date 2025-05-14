package com.hadoop.bplustree.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * Lớp đại diện cho node nội bộ trong B+ tree
 */
public class InternalNode extends TreeNode {

    private List<TreeNode> children;

    public InternalNode(int order) {
        super(order);
        this.children = new ArrayList<>();
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    /**
     * Tìm node con chứa khóa
     */
    public TreeNode findChild(long key) {
        int i = 0;
        while (i < keys.size() && key >= keys.get(i)) {
            i++;
        }
        return children.get(i);
    }

    /**
     * Chèn cặp khóa-child mới
     */
    public void insertKeyChild(long key, TreeNode child) {
        int pos = findKeyPosition(key);

        keys.add(pos, key);
        children.add(pos + 1, child);
        child.setParent(this);
    }

    /**
     * Chia node
     */
    public InternalNode split() {
        int mid = keys.size() / 2;
        InternalNode newNode = new InternalNode(order);

        // Chuyển các khóa và con sang node mới
        for (int i = mid + 1; i < keys.size(); i++) {
            newNode.keys.add(keys.get(i));
        }

        for (int i = mid + 1; i < children.size(); i++) {
            TreeNode child = children.get(i);
            newNode.children.add(child);
            child.setParent(newNode);
        }

        // Cập nhật node hiện tại
        keys.subList(mid, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();

        return newNode;
    }

    /**
     * Lấy khóa giữa
     */
    public long getMidKey() {
        return keys.get(keys.size() / 2);
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    /**
     * Thêm node con
     */
    public void addChild(TreeNode child) {
        children.add(child);
        child.setParent(this);
    }
}
package com.hadoop.bplustree.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing an internal node in the B+ tree structure
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
     * Locates the child node containing the specified key
     */
    public TreeNode findChild(long key) {
        int i = 0;
        while (i < keys.size() && key >= keys.get(i)) {
            i++;
        }
        return children.get(i);
    }

    /**
     * Inserts a new key-child pair into the node
     */
    public void insertKeyChild(long key, TreeNode child) {
        int pos = findKeyPosition(key);

        keys.add(pos, key);
        children.add(pos + 1, child);
        child.setParent(this);
    }

    /**
     * Splits the node when it exceeds capacity
     */
    public InternalNode split() {
        int mid = keys.size() / 2;
        InternalNode newNode = new InternalNode(order);

        // Move keys and children to new node
        for (int i = mid + 1; i < keys.size(); i++) {
            newNode.keys.add(keys.get(i));
        }

        for (int i = mid + 1; i < children.size(); i++) {
            TreeNode child = children.get(i);
            newNode.children.add(child);
            child.setParent(newNode);
        }

        // Update current node
        keys.subList(mid, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();

        return newNode;
    }

    /**
     * Retrieves the median key for node splitting
     */
    public long getMidKey() {
        return keys.get(keys.size() / 2);
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    /**
     * Adds a child node to the current node
     */
    public void addChild(TreeNode child) {
        children.add(child);
        child.setParent(this);
    }
}
package com.hadoop.bplustree.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class representing a node in the B+ tree structure
 */
public abstract class TreeNode implements Serializable {

    protected final int order; // Order of the B+ tree
    protected List<Long> keys; // List of keys
    protected TreeNode parent; // Parent node

    public TreeNode(int order) {
        this.order = order;
        this.keys = new ArrayList<>();
        this.parent = null;
    }

    /**
     * Determines if the node is a leaf node
     * @return true if the node is a leaf, false otherwise
     */
    public abstract boolean isLeaf();

    /**
     * Locates the position of a key within the node
     * @param key The key to locate
     * @return The index of the key or the position where it should be inserted
     */
    protected int findKeyPosition(long key) {
        int i = 0;
        while (i < keys.size() && keys.get(i) < key) {
            i++;
        }
        return i;
    }

    /**
     * Checks if the node has reached its maximum capacity
     * @return true if the node is full, false otherwise
     */
    public boolean isFull() {
        return keys.size() >= order - 1;
    }

    /**
     * Retrieves the list of keys in the node
     * @return The list of keys
     */
    public List<Long> getKeys() {
        return keys;
    }

    /**
     * Retrieves the parent node
     * @return The parent node
     */
    public TreeNode getParent() {
        return parent;
    }

    /**
     * Sets the parent node
     * @param parent The new parent node
     */
    public void setParent(TreeNode parent) {
        this.parent = parent;
    }
}
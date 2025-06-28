package com.hadoop.bplustree.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing a leaf node in the B+ tree structure
 */
public class LeafNode extends TreeNode {

    private List<String> values;
    private LeafNode next;

    public LeafNode(int order) {
        super(order);
        this.values = new ArrayList<>();
        this.next = null;
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    /**
     * Inserts a key-value pair into the node
     */
    public int insertKeyValue(long key, String value) {
        int pos = findKeyPosition(key);

        if (pos < keys.size() && keys.get(pos) == key) {
            values.set(pos, value);
            return pos;
        }

        keys.add(pos, key);
        values.add(pos, value);

        return pos;
    }

    /**
     * Retrieves the value associated with the specified key
     */
    public String findValue(long key) {
        int pos = findKeyPosition(key);

        if (pos < keys.size() && keys.get(pos) == key) {
            return values.get(pos);
        }

        return null;
    }

    /**
     * Splits the node when it exceeds capacity
     */
    public LeafNode split() {
        int mid = keys.size() / 2;
        LeafNode newNode = new LeafNode(order);

        for (int i = mid; i < keys.size(); i++) {
            newNode.keys.add(keys.get(i));
            newNode.values.add(values.get(i));
        }

        keys.subList(mid, keys.size()).clear();
        values.subList(mid, values.size()).clear();

        newNode.next = this.next;
        this.next = newNode;

        return newNode;
    }

    public List<String> getValues() {
        return values;
    }

    public LeafNode getNext() {
        return next;
    }

    public void setNext(LeafNode next) {
        this.next = next;
    }
}
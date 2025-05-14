package com.hadoop.bplustree.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * Lớp đại diện cho node lá trong B+ tree
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
     * Chèn cặp khóa-giá trị
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
     * Tìm giá trị cho khóa
     */
    public String findValue(long key) {
        int pos = findKeyPosition(key);

        if (pos < keys.size() && keys.get(pos) == key) {
            return values.get(pos);
        }

        return null;
    }

    /**
     * Chia node
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
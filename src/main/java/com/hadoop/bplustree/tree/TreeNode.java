package com.hadoop.bplustree.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Lớp trừu tượng đại diện cho node trong B+ tree
 */
public abstract class TreeNode implements Serializable {

    protected final int order; // Bậc của B+ tree
    protected List<Long> keys; // Danh sách các khóa
    protected TreeNode parent; // Node cha

    public TreeNode(int order) {
        this.order = order;
        this.keys = new ArrayList<>();
        this.parent = null;
    }

    /**
     * Kiểm tra node có phải là node lá không
     * @return true nếu là node lá, false nếu không
     */
    public abstract boolean isLeaf();

    /**
     * Tìm kiếm vị trí của khóa trong node
     * @param key Khóa cần tìm
     * @return Chỉ số của khóa hoặc vị trí khóa nên được chèn
     */
    protected int findKeyPosition(long key) {
        int i = 0;
        while (i < keys.size() && keys.get(i) < key) {
            i++;
        }
        return i;
    }

    /**
     * Kiểm tra node có đầy không
     * @return true nếu node đầy, false nếu không
     */
    public boolean isFull() {
        return keys.size() >= order - 1;
    }

    /**
     * Lấy danh sách các khóa
     * @return Danh sách các khóa
     */
    public List<Long> getKeys() {
        return keys;
    }

    /**
     * Lấy node cha
     * @return Node cha
     */
    public TreeNode getParent() {
        return parent;
    }

    /**
     * Đặt node cha
     * @param parent Node cha mới
     */
    public void setParent(TreeNode parent) {
        this.parent = parent;
    }
}
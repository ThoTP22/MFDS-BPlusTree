package com.hadoop.bplustree.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thuật toán QuickSelect để tìm phần tử thứ k
 */
public class QuickSelect {

    private static final Logger LOG = Logger.getLogger(QuickSelect.class.getName());

    /**
     * Tìm phần tử thứ k nhỏ nhất trong danh sách
     */
    public static long find(List<Long> data, int k) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data list cannot be null or empty");
        }

        if (k < 0 || k >= data.size()) {
            throw new IllegalArgumentException("k must be in [0, " + (data.size() - 1) + "]");
        }

        try {
            return findKthElement(data, 0, data.size() - 1, k);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error in QuickSelect: " + e.getMessage(), e);

            // Fallback: Sequential QuickSelect
            return findKthElement(new ArrayList<>(data), 0, data.size() - 1, k);
        }
    }

    /**
     * Thuật toán QuickSelect đệ quy
     */
    private static long findKthElement(List<Long> data, int left, int right, int k) {
        if (left == right) {
            return data.get(left);
        }

        // Chọn pivot và phân hoạch
        int pivotIndex = partition(data, left, right);

        if (k == pivotIndex) {
            return data.get(k);
        } else if (k < pivotIndex) {
            return findKthElement(data, left, pivotIndex - 1, k);
        } else {
            return findKthElement(data, pivotIndex + 1, right, k);
        }
    }

    /**
     * Phân hoạch dựa trên pivot
     */
    private static int partition(List<Long> data, int left, int right) {
        // Sử dụng Median-of-3 để chọn pivot tốt hơn
        int mid = left + (right - left) / 2;

        // Sắp xếp left, mid, right
        if (data.get(left) > data.get(mid)) swap(data, left, mid);
        if (data.get(left) > data.get(right)) swap(data, left, right);
        if (data.get(mid) > data.get(right)) swap(data, mid, right);

        // Pivot là giá trị ở giữa
        long pivot = data.get(mid);

        // Di chuyển pivot đến right-1
        swap(data, mid, right);

        // Phân hoạch
        int i = left;
        for (int j = left; j < right; j++) {
            if (data.get(j) <= pivot) {
                swap(data, i, j);
                i++;
            }
        }

        // Đặt pivot vào vị trí cuối cùng
        swap(data, i, right);
        return i;
    }

    /**
     * Đổi chỗ hai phần tử trong danh sách
     */
    private static void swap(List<Long> data, int i, int j) {
        if (i != j) {
            long temp = data.get(i);
            data.set(i, data.get(j));
            data.set(j, temp);
        }
    }
}
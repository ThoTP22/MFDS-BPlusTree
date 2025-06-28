package com.hadoop.bplustree.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * QuickSelect algorithm implementation for finding the kth element
 */
public class QuickSelect {

    private static final Logger LOG = Logger.getLogger(QuickSelect.class.getName());

    /**
     * Finds the kth smallest element in the list
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
     * Recursive QuickSelect algorithm implementation
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
     * Partition based on pivot selection
     */
    private static int partition(List<Long> data, int left, int right) {
        // Use Median-of-3 for better pivot selection
        int mid = left + (right - left) / 2;

        // Sort left, mid, right
        if (data.get(left) > data.get(mid)) swap(data, left, mid);
        if (data.get(left) > data.get(right)) swap(data, left, right);
        if (data.get(mid) > data.get(right)) swap(data, mid, right);

        // Pivot is the middle value
        long pivot = data.get(mid);

        // Move pivot to right-1
        swap(data, mid, right);

        // Partition
        int i = left;
        for (int j = left; j < right; j++) {
            if (data.get(j) <= pivot) {
                swap(data, i, j);
                i++;
            }
        }

        // Place pivot in final position
        swap(data, i, right);
        return i;
    }

    /**
     * Swaps two elements in the list
     */
    private static void swap(List<Long> data, int i, int j) {
        if (i != j) {
            long temp = data.get(i);
            data.set(i, data.get(j));
            data.set(j, temp);
        }
    }
}
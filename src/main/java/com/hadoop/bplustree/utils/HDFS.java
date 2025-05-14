package com.hadoop.bplustree.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Tiện ích làm việc với HDFS
 */
public class HDFS {

    private static final Logger LOG = Logger.getLogger(HDFS.class.getName());

    /**
     * Đọc file từ HDFS
     */
    public static List<String> readFile(Path path, Configuration conf) throws IOException {
        List<String> lines = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);

        try (FSDataInputStream in = fs.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        return lines;
    }

    /**
     * Ghi file vào HDFS
     */
    public static void writeFile(Path path, List<String> content, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        try (FSDataOutputStream out = fs.create(path);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

            for (String line : content) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    /**
     * Kiểm tra tồn tại
     */
    public static boolean exists(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(path);
    }

    /**
     * Xóa file hoặc thư mục
     */
    public static void delete(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
    }
}
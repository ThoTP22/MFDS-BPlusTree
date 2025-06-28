package com.hadoop.bplustree.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * HDFS Utility Class
 * Provides methods for HDFS operations
 */
public class HDFS {

    private static final Logger LOG = Logger.getLogger(HDFS.class.getName());

    /**
     * Reads a file from HDFS
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
     * Writes a file to HDFS
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
     * Checks existence
     */
    public static boolean exists(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(path);
    }

    /**
     * Deletes a file or directory
     */
    public static void delete(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
    }

    /**
     * Deletes a directory in HDFS
     */
    public static boolean deleteDirectory(String path) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path hdfsPath = new Path(path);
            
            if (fs.exists(hdfsPath)) {
                return fs.delete(hdfsPath, true);
            }
            return true;
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to delete directory: " + path, e);
            return false;
        }
    }

    /**
     * Creates a directory in HDFS
     */
    public static boolean createDirectory(String path) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path hdfsPath = new Path(path);
            
            if (!fs.exists(hdfsPath)) {
                return fs.mkdirs(hdfsPath);
            }
            return true;
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to create directory: " + path, e);
            return false;
        }
    }
}
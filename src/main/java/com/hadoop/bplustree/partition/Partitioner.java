package com.hadoop.bplustree.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Custom Partitioner for balanced data distribution across Reducers
 */
public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<LongWritable, Text>
        implements Configurable {

    private static final Logger LOG = Logger.getLogger(Partitioner.class.getName());
    private Configuration conf;
    private long[] points;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String str = conf.get("partition.points");

        if (str != null && !str.isEmpty()) {
            points = PartitionInfo.parse(str);
            LOG.info("Initialized with points: " + Arrays.toString(points));
        } else {
            LOG.warning("No partition points found - using default partitioning");
            points = new long[0];
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(LongWritable key, Text value, int numTasks) {
        if (numTasks == 1) {
            return 0;
        }

        if (points == null || points.length == 0) {
            return (int) ((key.get() & Long.MAX_VALUE) % numTasks);
        }

        long val = key.get();

        for (int i = 0; i < points.length; i++) {
            if (val <= points[i]) {
                return i;
            }
        }

        return points.length;
    }
}
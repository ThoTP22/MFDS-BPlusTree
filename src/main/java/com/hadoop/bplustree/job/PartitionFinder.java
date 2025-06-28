package com.hadoop.bplustree.job;

import com.hadoop.bplustree.partition.PartitionInfo;
import com.hadoop.bplustree.partition.QuickSelect;
import com.hadoop.bplustree.utils.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Phase 1: Multi-threaded Partition Point Discovery
 */
public class PartitionFinder extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(PartitionFinder.class.getName());

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: PartitionFinder <input_path> <output_path> <num_partitions>");
            return 1;
        }

        String input = args[0];
        String output = args[1];
        int parts = Integer.parseInt(args[2]);

        Configuration conf = getConf();
        conf.setInt("num.parts", parts);

        // Number of threads for multi-threading
        int threadsPerMapper = conf.getInt("threads.per.mapper", Runtime.getRuntime().availableProcessors());

        LOG.info("======= Phase 1: Partition Finding =======");
        LOG.info("Input: " + input);
        LOG.info("Output: " + output);
        LOG.info("Partitions: " + parts);
        LOG.info("Threads per mapper: " + threadsPerMapper);
        LOG.info("Strategy: Multi-threaded chunk processing with QuickSelect");
        LOG.info("==========================================");

        // Start timing
        Timer.start("phase1");

        // Delete output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output + "/points");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Set up MapReduce job
        Job job = Job.getInstance(conf, "Find Partition Points");
        job.setJarByClass(PartitionFinder.class);

        // Configure Mapper and Reducer
        job.setMapperClass(LocalPartitionMapper.class);
        job.setReducerClass(GlobalPartitionReducer.class);

        // Configure key-value types
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // Single reducer to find global partition points
        job.setNumReduceTasks(1);

        // Set paths
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, outputPath);

        // Execute job
        boolean success = job.waitForCompletion(true);

        if (success) {
            LOG.info("MapReduce job completed successfully");

            // Read results and save to points.txt
            Path pointsPath = new Path(output + "/points/part-r-00000");
            long[] points = PartitionInfo.load(pointsPath, conf);
            PartitionInfo.save(points, new Path(output + "/points.txt"), conf);

            LOG.info("Saved " + points.length + " global partition points to " + output + "/points.txt");
        } else {
            LOG.severe("MapReduce job failed");
            return 1;
        }

        long phase1Time = Timer.end("phase1");
        LOG.info("Phase 1 completed in " + (phase1Time / 1000.0) + " seconds");

        return 0;
    }

    /**
     * Mapper for local partition point discovery
     */
    public static class LocalPartitionMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        private List<Long> data = new ArrayList<>();
        private int parts;
        private int threadsPerMapper;
        private NullWritable nullKey = NullWritable.get();
        private long processedRecords = 0;
        private static final long LOG_INTERVAL = 1000000; // Log every 1 million records

        @Override
        protected void setup(Context context) {
            parts = context.getConfiguration().getInt("num.parts", 4);
            threadsPerMapper = context.getConfiguration().getInt("threads.per.mapper",
                    Runtime.getRuntime().availableProcessors());
            LOG.info("LocalPartitionMapper starting with " + threadsPerMapper + " threads");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            // Read number from text line
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                try {
                    data.add(Long.parseLong(line));

                    processedRecords++;
                    if (processedRecords % LOG_INTERVAL == 0) {
                        LOG.info("Processed " + processedRecords + " records");
                    }
                } catch (NumberFormatException e) {
                    // Skip non-numeric lines
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (data.isEmpty()) {
                LOG.warning("No data collected in mapper");
                return;
            }

            LOG.info("Starting multi-threaded partition finding with " + threadsPerMapper + " threads on " + data.size() + " values");

            // Divide data into chunks based on number of threads
            int chunkSize = data.size() / threadsPerMapper;
            if (chunkSize == 0) chunkSize = 1;

            List<List<Long>> chunks = new ArrayList<>();
            for (int i = 0; i < threadsPerMapper; i++) {
                int start = i * chunkSize;
                int end = (i == threadsPerMapper - 1) ? data.size() : (i + 1) * chunkSize;
                if (start < data.size()) {
                    chunks.add(new ArrayList<>(data.subList(start, end)));
                }
            }

            // Create and run threads
            ExecutorService threadPool = Executors.newFixedThreadPool(threadsPerMapper);
            List<Future<List<Long>>> chunkFutures = new ArrayList<>();

            // Each thread processes a chunk using QuickSelect
            for (final List<Long> chunk : chunks) {
                chunkFutures.add(threadPool.submit(() -> {
                    List<Long> localPoints = new ArrayList<>();
                    for (int i = 1; i < parts; i++) {
                        int k = i * chunk.size() / parts;
                        if (k < chunk.size()) {
                            // Use QuickSelect instead of sorting
                            long point = QuickSelect.find(chunk, k);
                            localPoints.add(point);
                        }
                    }
                    return localPoints;
                }));
            }

            // Collect results from all threads
            List<Long> allLocalPoints = new ArrayList<>();
            try {
                for (Future<List<Long>> future : chunkFutures) {
                    allLocalPoints.addAll(future.get());
                }
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error in parallel partition finding", e);
            } finally {
                threadPool.shutdown();
            }

            // Send local partition points to reducer
            for (Long point : allLocalPoints) {
                context.write(nullKey, new LongWritable(point));
            }

            LOG.info("Found " + allLocalPoints.size() + " local partition points");
        }
    }

    /**
     * Reducer for global partition point determination
     */
    public static class GlobalPartitionReducer extends Reducer<NullWritable, LongWritable, LongWritable, LongWritable> {
        private int parts;
        private List<Long> allPoints = new ArrayList<>();

        @Override
        protected void setup(Context context) {
            parts = context.getConfiguration().getInt("num.parts", 4);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // Collect all partition points
            for (LongWritable value : values) {
                allPoints.add(value.get());
            }

            // Sort points
            Collections.sort(allPoints);

            // Find global partition points
            List<Long> globalPoints = new ArrayList<>();
            for (int i = 1; i < parts; i++) {
                int k = i * allPoints.size() / parts;
                if (k < allPoints.size()) {
                    globalPoints.add(allPoints.get(k));
                }
            }

            // Output global partition points
            for (Long point : globalPoints) {
                context.write(new LongWritable(point), new LongWritable(point));
            }

            LOG.info("Found " + globalPoints.size() + " global partition points");
        }
    }
}
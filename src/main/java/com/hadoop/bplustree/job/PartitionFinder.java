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
 * Phase 1: Tìm điểm phân vùng sử dụng xử lý đa luồng
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

        // Số luồng cho multi-threading
        int threadsPerMapper = conf.getInt("threads.per.mapper", Runtime.getRuntime().availableProcessors());

        LOG.info("======= Phase 1: Partition Finding =======");
        LOG.info("Input: " + input);
        LOG.info("Output: " + output);
        LOG.info("Partitions: " + parts);
        LOG.info("Threads per mapper: " + threadsPerMapper);
        LOG.info("Strategy: Multi-threaded chunk processing with QuickSelect");
        LOG.info("==========================================");

        // Bắt đầu đo thời gian
        Timer.start("phase1");

        // Xóa thư mục đầu ra nếu đã tồn tại
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output + "/points");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Thiết lập job MapReduce
        Job job = Job.getInstance(conf, "Find Partition Points");
        job.setJarByClass(PartitionFinder.class);

        // Cấu hình Mapper và Reducer
        job.setMapperClass(LocalPartitionMapper.class);
        job.setReducerClass(GlobalPartitionReducer.class);

        // Cấu hình kiểu key-value
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // Một reducer duy nhất để tìm điểm phân vùng toàn cục
        job.setNumReduceTasks(1);

        // Thiết lập đường dẫn
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, outputPath);

        // Thực hiện job
        boolean success = job.waitForCompletion(true);

        if (success) {
            LOG.info("MapReduce job completed successfully");

            // Đọc kết quả và lưu vào points.txt
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
     * Mapper tìm điểm phân vùng cục bộ
     */
    public static class LocalPartitionMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        private List<Long> data = new ArrayList<>();
        private int parts;
        private int threadsPerMapper;
        private NullWritable nullKey = NullWritable.get();
        private long processedRecords = 0;
        private static final long LOG_INTERVAL = 1000000; // Log mỗi 1 triệu bản ghi

        @Override
        protected void setup(Context context) {
            parts = context.getConfiguration().getInt("num.parts", 4);
            threadsPerMapper = context.getConfiguration().getInt("threads.per.mapper",
                    Runtime.getRuntime().availableProcessors());
            LOG.info("LocalPartitionMapper starting with " + threadsPerMapper + " threads");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            // Đọc số từ dòng văn bản
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                try {
                    data.add(Long.parseLong(line));

                    processedRecords++;
                    if (processedRecords % LOG_INTERVAL == 0) {
                        LOG.info("Processed " + processedRecords + " records");
                    }
                } catch (NumberFormatException e) {
                    // Bỏ qua dòng không phải số
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

            // Chia dữ liệu thành các chunk dựa trên số luồng
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

            // Tạo và chạy các luồng
            ExecutorService threadPool = Executors.newFixedThreadPool(threadsPerMapper);
            List<Future<List<Long>>> chunkFutures = new ArrayList<>();

            // Mỗi luồng xử lý một chunk sử dụng QuickSelect
            for (final List<Long> chunk : chunks) {
                chunkFutures.add(threadPool.submit(() -> {
                    List<Long> localPoints = new ArrayList<>();
                    for (int i = 1; i < parts; i++) {
                        int k = i * chunk.size() / parts;
                        if (k < chunk.size()) {
                            // Sử dụng QuickSelect thay vì sắp xếp
                            long point = QuickSelect.find(chunk, k);
                            localPoints.add(point);
                        }
                    }
                    return localPoints;
                }));
            }

            // Thu thập kết quả từ tất cả các luồng
            List<Long> allLocalPoints = new ArrayList<>();
            try {
                for (Future<List<Long>> future : chunkFutures) {
                    allLocalPoints.addAll(future.get());
                }
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error processing chunks: " + e.getMessage(), e);
            } finally {
                threadPool.shutdown();
            }

            // Thay vì sắp xếp, sử dụng QuickSelect để tìm điểm phân vùng cuối cùng
            for (int i = 1; i < parts; i++) {
                int k = i * allLocalPoints.size() / parts;
                if (k < allLocalPoints.size()) {
                    long localPartitionPoint = QuickSelect.find(allLocalPoints, k);
                    context.write(nullKey, new LongWritable(localPartitionPoint));
                    LOG.info("Emitted local partition point " + (i-1) + ": " + localPartitionPoint);
                }
            }

            LOG.info("LocalPartitionMapper completed, processed " + data.size() + " records total");
        }
    }

    /**
     * Reducer tìm điểm phân vùng toàn cục
     */
    public static class GlobalPartitionReducer extends Reducer<NullWritable, LongWritable, LongWritable, LongWritable> {
        private int parts;

        @Override
        protected void setup(Context context) {
            parts = context.getConfiguration().getInt("num.parts", 4);
            LOG.info("GlobalPartitionReducer starting for " + parts + " partitions");
        }

        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // Thu thập tất cả các điểm phân vùng cục bộ
            List<Long> allPartitionPoints = new ArrayList<>();
            for (LongWritable value : values) {
                allPartitionPoints.add(value.get());
            }

            LOG.info("Collected " + allPartitionPoints.size() + " local partition points");

            if (allPartitionPoints.isEmpty()) {
                LOG.warning("No local partition points were collected");
                return;
            }

            // Tìm các điểm phân vùng toàn cục sử dụng QuickSelect
            long[] globalPoints = new long[parts - 1];
            for (int i = 0; i < parts - 1; i++) {
                int k = i * allPartitionPoints.size() / (parts - 1);

                // Đảm bảo k hợp lệ
                k = Math.min(k, allPartitionPoints.size() - 1);
                k = Math.max(k, 0);

                globalPoints[i] = QuickSelect.find(allPartitionPoints, k);

                context.write(new LongWritable(i), new LongWritable(globalPoints[i]));
                LOG.info("Global partition point " + i + ": " + globalPoints[i]);
            }

            LOG.info("Found " + globalPoints.length + " global partition points");
        }
    }
}
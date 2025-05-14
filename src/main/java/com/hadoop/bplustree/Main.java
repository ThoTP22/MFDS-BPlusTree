package com.hadoop.bplustree;

import com.hadoop.bplustree.job.PartitionFinder;
import com.hadoop.bplustree.job.DataDistributor;
import com.hadoop.bplustree.job.MetadataAggregator;
import com.hadoop.bplustree.utils.Timer;
import com.hadoop.bplustree.utils.ConfigManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Main executor điều phối toàn bộ quy trình xây dựng B+tree phân tán
 */
public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());
    private static final String REPORT_FILE = "timing_report.txt";
    private static final String LOG_FILE = "bplustree-hadoop-%g.log";
    private static final int LOG_SIZE_LIMIT = 10 * 1024 * 1024; // 10 MB
    private static final int LOG_FILE_COUNT = 5;
    private static final long MEMORY_THRESHOLD = 100 * 1024 * 1024; // 100 MB

    static {
        // Cấu hình logging
        configureLogging();
    }

    private static void configureLogging() {
        try {
            // Xóa handlers mặc định
            Logger rootLogger = Logger.getLogger("");
            for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
                rootLogger.removeHandler(handler);
            }

            // Thêm console handler với formatter tùy chỉnh
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setFormatter(new SimpleFormatter() {
                private static final String format = "[%1$tF %1$tT] [%2$s] %3$s %n";

                @Override
                public synchronized String format(java.util.logging.LogRecord record) {
                    return String.format(format,
                            new java.util.Date(record.getMillis()),
                            record.getLevel().getName(),
                            record.getMessage()
                    );
                }
            });

            // Thêm file handler cho việc ghi log vào file
            FileHandler fileHandler = new FileHandler(LOG_FILE, LOG_SIZE_LIMIT, LOG_FILE_COUNT, true);
            fileHandler.setFormatter(new SimpleFormatter());

            // Đặt level cho root logger
            rootLogger.setLevel(Level.INFO);
            rootLogger.addHandler(consoleHandler);
            rootLogger.addHandler(fileHandler);

            LOG.info("Logging initialized with console and file output");
        } catch (Exception e) {
            System.err.println("Failed to initialize logging: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Hiển thị thông tin sử dụng
     */
    private static void showUsage() {
        System.err.println("Usage: Main <input> <output> <partitions> <order> [configFile]");
        System.err.println("  input:      Đường dẫn HDFS đến file dữ liệu đầu vào");
        System.err.println("  output:     Đường dẫn HDFS đến thư mục đầu ra");
        System.err.println("  partitions: Số lượng phân vùng (partitions)");
        System.err.println("  order:      Bậc của cây B+Tree");
        System.err.println("  configFile: Đường dẫn tới file cấu hình (tùy chọn)");
        System.err.println();
        System.err.println("Example: Main /user/data/input.txt /user/data/output 4 100");
        System.err.println("Or with config: Main /user/data/input.txt /user/data/output 4 100 config.properties");
    }

    /**
     * Kiểm tra đối số đầu vào
     */
    private static boolean validateArgs(String[] args) {
        if (args.length < 4) {
            LOG.severe("Thiếu đối số. Cần ít nhất 4 đối số.");
            return false;
        }

        // Log tất cả các đối số để debug
        LOG.info("Validating arguments: [" +
                "input=" + args[0] + ", " +
                "output=" + args[1] + ", " +
                "partitions=" + args[2] + ", " +
                "order=" + args[3] + "]");

        try {
            // Xử lý phân tích đối số chi tiết hơn
            String partitionsStr = args[2].trim();
            String orderStr = args[3].trim();

            LOG.info("Parsing partitions from: '" + partitionsStr + "'");
            int partitions = Integer.parseInt(partitionsStr);
            LOG.info("Successfully parsed partitions: " + partitions);

            if (partitions <= 0) {
                LOG.severe("Số lượng phân vùng phải lớn hơn 0");
                return false;
            }

            LOG.info("Parsing order from: '" + orderStr + "'");
            int order = Integer.parseInt(orderStr);
            LOG.info("Successfully parsed order: " + order);

            if (order < 3) {
                LOG.severe("Bậc của cây B+Tree phải lớn hơn hoặc bằng 3");
                return false;
            }

            LOG.info("Arguments validated successfully");
            return true;
        } catch (NumberFormatException e) {
            LOG.severe("Đối số partitions và order phải là số nguyên. Chi tiết lỗi: " + e.getMessage());

            // Thêm thông tin về các đối số để debug
            LOG.severe("Args[2]=" + args[2] + ", length=" + args[2].length() +
                    ", bytes=" + Arrays.toString(args[2].getBytes()));
            LOG.severe("Args[3]=" + args[3] + ", length=" + args[3].length() +
                    ", bytes=" + Arrays.toString(args[3].getBytes()));

            return false;
        }
    }

    /**
     * Hiển thị thông tin hệ thống
     */
    private static void showSystemInfo() {
        LOG.info("=== System Information ===");
        LOG.info("Java version: " + System.getProperty("java.version"));
        LOG.info("Java home: " + System.getProperty("java.home"));
        LOG.info("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        LOG.info("User: " + System.getProperty("user.name"));
        LOG.info("Available processors: " + Runtime.getRuntime().availableProcessors());
        LOG.info("Max memory: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB");

        // Thử lấy thông tin về Hadoop
        try {
            Process process = Runtime.getRuntime().exec("hadoop version");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                if ((line = reader.readLine()) != null) {
                    LOG.info("Hadoop version: " + line);
                }
            }
        } catch (Exception e) {
            LOG.info("Could not determine Hadoop version: " + e.getMessage());
        }

        LOG.info("===========================");
    }

    /**
     * Kiểm tra bộ nhớ có đủ không
     */
    private static boolean checkMemory() {
        long freeMemory = Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        if (freeMemory < MEMORY_THRESHOLD) {
            LOG.warning("Cảnh báo: Bộ nhớ trống thấp, chỉ còn " + (freeMemory / (1024 * 1024)) + " MB");
            return false;
        }
        return true;
    }

    /**
     * Kiểm tra thư mục đầu ra
     */
    private static void checkOutputDirectory(String output, Configuration conf) throws IOException {
        Path outputPath = new Path(output);
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            LOG.warning("Thư mục đầu ra đã tồn tại: " + output);
            LOG.warning("Dữ liệu hiện có sẽ bị ghi đè");
        }
    }

    /**
     * Tính toán và hiển thị thời gian chạy
     */
    private static void reportExecutionTime(long startTime) {
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        long hours = TimeUnit.MILLISECONDS.toHours(duration);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(duration) % 60;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(duration) % 60;

        LOG.info(String.format("Tổng thời gian thực thi: %d giờ, %d phút, %d giây (%d ms)",
                hours, minutes, seconds, duration));
    }

    /**
     * Hàm main
     */
    public static void main(String[] args) {
        // Đánh dấu thời điểm bắt đầu
        long startTime = System.currentTimeMillis();

        try {
            LOG.info("Start B+Tree distributed construction...");
            showSystemInfo();

            // Kiểm tra bộ nhớ
            if (!checkMemory()) {
                LOG.warning("Tiếp tục thực thi mặc dù bộ nhớ thấp");
            }

            // Kiểm tra đối số đầu vào
            if (!validateArgs(args)) {
                showUsage();
                System.exit(1);
            }

            String input = args[0];
            String output = args[1];
            int partitions = Integer.parseInt(args[2]);
            int order = Integer.parseInt(args[3]);

            // Tải cấu hình từ file nếu có
            String configFile = args.length > 4 ? args[4] : null;
            Properties props = new Properties();

            if (configFile != null) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                    LOG.info("Loaded configuration from: " + configFile);
                } catch (IOException e) {
                    LOG.warning("Could not load config file: " + e.getMessage());
                }
            }

            // Số luồng mặc định dựa vào CPU cores hoặc cấu hình
            int threads = Integer.parseInt(props.getProperty("threads",
                    String.valueOf(Runtime.getRuntime().availableProcessors())));

            // Phương pháp xây dựng từ cấu hình
            String buildingMethod = props.getProperty("building.method", "bottom-up");

            LOG.info("====== B+Tree Construction Starting ======");
            LOG.info("Input: " + input);
            LOG.info("Output: " + output);
            LOG.info("Partitions: " + partitions);
            LOG.info("Tree order: " + order);
            LOG.info("Threads per mapper/reducer: " + threads);
            LOG.info("Building method: " + buildingMethod);
            LOG.info("======================================");

            // Khởi tạo timer cho toàn bộ quá trình
            Timer.start("total");

            // Khởi tạo ConfigManager để quản lý cấu hình
            ConfigManager configManager = new ConfigManager();

            // Thiết lập cấu hình
            Configuration conf = configManager.createConfiguration(props);

            // Cấu hình số luồng
            conf.setInt("threads.per.mapper", threads);
            conf.setInt("threads.per.reducer", threads);

            // Đặt phương pháp xây dựng
            conf.set("building.method", buildingMethod);

            // Kiểm tra thư mục đầu ra
            checkOutputDirectory(output, conf);

            try {
                // Phase 1: Tìm điểm phân vùng
                LOG.info("======= Executing Phase 1: Finding partition points =======");
                Timer.start("phase1");

                String[] phase1Args = {input, output, String.valueOf(partitions)};
                LOG.info("Phase 1 arguments: " + Arrays.toString(phase1Args));

                int result = ToolRunner.run(conf, new PartitionFinder(), phase1Args);

                if (result != 0) {
                    throw new RuntimeException("Phase 1: Finding partition points failed with code " + result);
                }

                long phase1Time = Timer.end("phase1");
                LOG.info("Phase 1 completed in " + (phase1Time / 1000.0) + " seconds");

                // Kiểm tra bộ nhớ trước khi chuyển sang phase 2
                if (!checkMemory()) {
                    LOG.warning("Tiếp tục thực thi mặc dù bộ nhớ thấp");
                    System.gc(); // Yêu cầu GC chạy
                }

                // Phase 2: Phân phối dữ liệu và xây dựng cây
                LOG.info("======= Executing Phase 2: Distributing data and building B+trees =======");
                Timer.start("phase2");

                String[] phase2Args = {
                        input,
                        output,
                        output + "/points.txt",
                        String.valueOf(order)
                };
                LOG.info("Phase 2 arguments: " + Arrays.toString(phase2Args));

                result = ToolRunner.run(conf, new DataDistributor(), phase2Args);

                if (result != 0) {
                    throw new RuntimeException("Phase 2: Distributing data and building B+trees failed with code " + result);
                }

                long phase2Time = Timer.end("phase2");
                LOG.info("Phase 2 completed in " + (phase2Time / 1000.0) + " seconds");

                // Kiểm tra bộ nhớ trước khi chuyển sang phase 3
                if (!checkMemory()) {
                    LOG.warning("Tiếp tục thực thi mặc dù bộ nhớ thấp");
                    System.gc(); // Yêu cầu GC chạy
                }

                // Phase 3: Tổng hợp metadata
                LOG.info("======= Executing Phase 3: Aggregating metadata =======");
                Timer.start("phase3");

                String[] phase3Args = {
                        output,
                        String.valueOf(partitions),
                        String.valueOf(order)
                };
                LOG.info("Phase 3 arguments: " + Arrays.toString(phase3Args));

                result = ToolRunner.run(conf, new MetadataAggregator(), phase3Args);

                if (result != 0) {
                    throw new RuntimeException("Phase 3: Aggregating metadata failed with code " + result);
                }

                long phase3Time = Timer.end("phase3");
                LOG.info("Phase 3 completed in " + (phase3Time / 1000.0) + " seconds");

            } catch (Exception e) {
                LOG.severe("Error: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }

            // Tính toán tổng thời gian
            long totalTime = Timer.end("total");
            LOG.info("Total execution time: " + (totalTime / 1000.0) + " seconds");

            // Báo cáo thời gian
            Timer.report();
            Timer.save(REPORT_FILE);

            LOG.info("B+tree construction completed successfully!");
            LOG.info("Results saved to: " + output);
            LOG.info("See timing report in: " + REPORT_FILE);

            // Báo cáo tổng thời gian thực thi
            reportExecutionTime(startTime);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Unhandled exception in main: " + e.getMessage(), e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
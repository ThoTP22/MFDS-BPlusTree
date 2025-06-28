package com.hadoop.bplustree.utils;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lớp quản lý cấu hình cho ứng dụng B+Tree
 */
public class ConfigManager {
    private static final Logger LOG = Logger.getLogger(ConfigManager.class.getName());

    // Các giá trị mặc định
    private static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();

    static {
        // Cấu hình Hadoop
        DEFAULT_CONFIG.put("hadoop.tmp.dir", "/tmp/hadoop-${user.name}");
        DEFAULT_CONFIG.put("mapreduce.framework.name", "yarn");
        DEFAULT_CONFIG.put("mapreduce.map.memory.mb", "2048");
        DEFAULT_CONFIG.put("mapreduce.reduce.memory.mb", "4096");

        // Cấu hình ứng dụng
        DEFAULT_CONFIG.put("threads.per.block",
                String.valueOf(Runtime.getRuntime().availableProcessors()));
        DEFAULT_CONFIG.put("building.method", "bottom-up");
        DEFAULT_CONFIG.put("tree.cache.size", "1000");
    }

    /**
     * Tạo cấu hình Hadoop với các giá trị mặc định
     */
    public Configuration createDefaultConfiguration() {
        Configuration conf = new Configuration();

        // Áp dụng các giá trị mặc định
        for (Map.Entry<String, String> entry : DEFAULT_CONFIG.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        return conf;
    }

    /**
     * Tạo cấu hình Hadoop từ Properties
     */
    public Configuration createConfiguration(Properties props) {
        Configuration conf = createDefaultConfiguration();

        if (props != null) {
            // Áp dụng các giá trị từ Properties
            for (String key : props.stringPropertyNames()) {
                String value = props.getProperty(key);
                if (value != null && !value.isEmpty()) {
                    conf.set(key, value);
                    LOG.info("Set configuration: " + key + " = " + value);
                }
            }
        }

        return conf;
    }

    /**
     * Tạo cấu hình Hadoop từ file
     */
    public Configuration createConfigurationFromFile(String configFile) {
        if (configFile == null || configFile.isEmpty()) {
            return createDefaultConfiguration();
        }

        Properties props = new Properties();
        File file = new File(configFile);

        if (file.exists() && file.canRead()) {
            try (FileInputStream fis = new FileInputStream(file)) {
                props.load(fis);
                LOG.info("Loaded " + props.size() + " properties from " + configFile);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Could not load config file: " + e.getMessage(), e);
            }
        } else {
            LOG.warning("Config file does not exist or is not readable: " + configFile);
        }

        return createConfiguration(props);
    }

    /**
     * Lưu cấu hình hiện tại ra log
     */
    public void logConfiguration(Configuration conf) {
        LOG.info("===== Current Configuration =====");

        // In các tham số quan trọng
        logConfigValue(conf, "mapreduce.framework.name");
        logConfigValue(conf, "mapreduce.map.memory.mb");
        logConfigValue(conf, "mapreduce.reduce.memory.mb");
        logConfigValue(conf, "threads.per.block");
        logConfigValue(conf, "building.method");
        logConfigValue(conf, "tree.cache.size");

        LOG.info("=================================");
    }

    /**
     * In một giá trị cấu hình
     */
    private void logConfigValue(Configuration conf, String key) {
        String value = conf.get(key);
        if (value != null) {
            LOG.info(key + " = " + value);
        } else {
            LOG.info(key + " = <not set>");
        }
    }
}
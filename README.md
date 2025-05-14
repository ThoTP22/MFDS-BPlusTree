# Distributed B+Tree Construction on Hadoop

This project implements a **scalable and efficient B+Tree construction** system over large datasets using the Hadoop MapReduce framework. It features multi-threaded QuickSelect-based partitioning, bottom-up B+Tree building, and metadata aggregation—all fully parallelizable across partitions.

## Features

* ✅ **Multi-phase execution**: Partitioning → Distribution → Metadata Aggregation
* 🚀 **Bottom-Up Tree Construction** for speed and memory efficiency
* 🌍 **Distributed & Scalable**: Built for Hadoop clusters
* 🧠 **Optimized partitioning** using **QuickSelect** instead of sampling
* 📊 Performance tracking with detailed time & memory logs
* 🔧 Configurable with external `.properties` file

## Environment

* Java Version: **17**
* Hadoop Version: **3.4.1**

## Project Structure

```
com.hadoop.bplustree
├── Main.java               // Main controller: coordinates all 3 phases
├── job/
│   ├── PartitionFinder.java        // Phase 1: Partition point calculation
│   ├── DataDistributor.java        // Phase 2: Data distribution & tree building
│   └── MetadataAggregator.java     // Phase 3: Metadata & report generation
├── partition/
│   ├── QuickSelect.java
│   ├── Partitioner.java
│   └── PartitionInfo.java
├── tree/
│   ├── BPlusTree.java
│   ├── TreeNode.java
│   ├── LeafNode.java
│   └── InternalNode.java
├── utils/
│   ├── ConfigManager.java
│   ├── Timer.java
│   └── HDFS.java
```

## Usage

### Run Command

```bash
hadoop jar bplustree.jar Main <input> <output> <num_partitions> <tree_order> [config.properties]
```

**Example:**

```bash
hadoop jar bplustree.jar Main /user/data/input.txt /user/data/output 4 100 config.properties
```

### Input Format

* Text file with one numeric key per line.

### Output

* `/output/data/`: partitioned and sorted key-value data
* `/output/tree/`: one B+Tree metadata file per partition
* `/output/subtrees.json`: metadata in JSON for integration
* `/output/index.html`: browsable summary report

## Configuration (Optional)

Set in a `.properties` file:

```properties
threads=8
building.method=bottom-up
sample.rate=0.1
batch.size=100000
tree.cache.size=1000
```

## Phases Breakdown

1. **Phase 1: Partitioning**

   * Uses `QuickSelect` to find global partition points.
   * Multi-threaded for efficiency.
   * Output: `points.txt`

2. **Phase 2: Data Distribution**

   * MapReduce sorts & partitions data.
   * Each reducer builds one B+Tree (bottom-up).
   * Output: B+Tree files in `/tree/`

3. **Phase 3: Metadata Aggregation**

   * Aggregates tree stats.
   * Generates `.json`, `.txt`, and `.html` reports

## Performance Logs

Timing and memory usage are logged via `Timer.java`. Outputs include:

* Console logs
* `timing_report.txt`
* `tree_stats.txt`, `summary_report.txt`
* `index.html` with visual summary

## License

MIT


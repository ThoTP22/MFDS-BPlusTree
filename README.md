# MFDS: An Enhanced MapReduce-based


Framework for Huge Available Data Storage and
Management&#x20;
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
* Maven Version: **3.9.9**

## Dataset Format

The dataset should be in **CSV format**, containing a single column of **integer values**. Example:

```
12453
34721
48213
...
```

Each row represents a single key to be indexed in the B+Tree.

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

## Setup on Hadoop Cluster

> 💡 If you haven't set up Hadoop yet, follow the official Apache guide:
> [How to set up a single-node Hadoop cluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)

### 1. Prepare Hadoop Environment

* Install Hadoop 3.4.1 (pseudo-distributed or fully distributed)
* Ensure Java 17 is installed and configured
* Format HDFS and start NameNode & DataNode

```bash
start-dfs.sh
start-yarn.sh
```

### 2. Package Project

Use Maven to build the JAR:

```bash
mvn clean package
```

The output will be in `target/bplustree-hadoop.jar`

### 3. Upload Input Data to HDFS

```bash
hdfs dfs -mkdir -p /user/yourname/input
hdfs dfs -put input.csv /user/yourname/input/
```

### 4. Run MapReduce Job

```bash
hadoop jar target/bplustree-hadoop.jar com.hadoop.bplustree.Main \
  /user/yourname/input/input.csv \
  /user/yourname/output \
  4 \
  100 \
  config.properties
```

### 5. Fetch Output

```bash
hdfs dfs -cat /user/yourname/output/subtrees.json
hdfs dfs -get /user/yourname/output ./local-output
```

## Configuration (Optional)

Set in a `.properties` file:

```properties
threads=8
building.method=bottom-up
batch.size=100000
tree.cache.size=1000
```

## Output

* `/output/data/`: partitioned and sorted key-value data
* `/output/tree/`: one B+Tree metadata file per partition
* `/output/subtrees.json`: metadata in JSON for integration
* `/output/index.html`: browsable summary report

## Performance Logs

Timing and memory usage are logged via `Timer.java`. Outputs include:

* Console logs
* `timing_report.txt`
* `tree_stats.txt`, `summary_report.txt`
* `index.html` with visual summary

## .gitignore Suggestions

```
.idea/
.vscode/
.DS_Store
.env
.env.local
.env.development.local
.env.test.local
target/
dataset/
report/
log/
output/
temp/
cache/
history/
```

## License

This project is licensed under the **MIT License**.



# Spark 补充 jar 清单（21 个 delta jar）

`spark-4.1.2-bin-without-hadoop` 发行版不含 Hive 支持相关 jar（`spark-hive`、Hive 2.3 client 及它们的依赖都不在其中）。需从官方 `spark-4.1.2-bin-hadoop3` 捆绑包的 `jars/` 中筛选出下列 21 个补充 jar。

> 注意：不能把 hadoop3 捆绑包的 `jars/` 整个拿来用（作 `spark.yarn.jars` 或 driver classpath），以免与集群自带的 Hadoop 依赖冲突或引入冗余 jar；下列清单中也不含任何 Hadoop jar。

## 两个用途

1. **HDFS `spark.yarn.jars`**：与 `$SPARK_HOME/jars/*` 一起上传到 `hdfs://mycluster/spark/spark-jars/`，供 YARN 容器（cluster 模式的 AM/driver 与所有 executor）加载：

   ```shell
   hdfs dfs -put /opt/spark-4.1.2/jars/* /opt/local-spark-jars/spark-driver-extra/* /spark/spark-jars/
   ```

2. **client 模式 driver**：这批 jar 放在提交节点的本地目录（如 `/opt/local-spark-jars/spark-driver-extra/`），client 模式下 driver 看不到 HDFS 的 `spark-jars`，需通过 `--driver-class-path "/opt/local-spark-jars/spark-driver-extra/*"` 或写进 `spark-defaults.conf` 的 `spark.driver.extraClassPath` 提供。

## 清单

```text
antlr-runtime-3.5.2.jar
datanucleus-api-jdo-4.2.4.jar
datanucleus-core-4.1.17.jar
datanucleus-rdbms-4.1.19.jar
hive-common-2.3.10.jar
hive-exec-2.3.10-core.jar
hive-metastore-2.3.10.jar
hive-serde-2.3.10.jar
hive-service-rpc-4.0.0.jar
hive-shims-0.23-2.3.10.jar
hive-shims-2.3.10.jar
hive-shims-common-2.3.10.jar
hive-shims-scheduler-2.3.10.jar
javax.jdo-3.2.0-m3.jar
jdo-api-3.0.1.jar
joda-time-2.14.0.jar
json-1.8.jar
libfb303-0.9.3.jar
libthrift-0.16.0.jar
spark-hive_2.13-4.1.2.jar
ST4-4.0.4.jar
```

## 说明

- 版本对应关系：jar 版本绑定 Spark 4.1.2 与 Hive 2.3.10——`spark-hive_2.13-4.1.2.jar` 按 Hive 2.3 编译，直接引用上述 Hive 2.3 client 类，升级 Spark 或 Hive 版本时需同步重新筛选。
- `hive-exec-2.3.10-core.jar` 是不含 optional 依赖的 core 变体，来自 hadoop3 捆绑包。
- 这批 jar 同时覆盖 SQL 运行器与 Scala 运行器的需求：`SparkSqlRunner` / `SparkScalaRunner` 脚本内执行 `spark.sql()` 需要 `HiveSessionStateBuilder`（在 `spark-hive` 中），client 模式下缺这批 jar 会报 `ClassNotFoundException: org.apache.spark.sql.hive.HiveSessionStateBuilder`。

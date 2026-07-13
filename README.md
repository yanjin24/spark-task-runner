# spark-task-runner

通用 Spark 任务执行器，支持 SQL 和 Scala 两种任务，通过 `--class` 选择入口：

- `com.example.spark.SparkSqlRunner`：执行 SQL 文件，按分号拆分语句依次执行；末条语句为 SELECT 时打印查询结果。
- `com.example.spark.SparkScalaRunner`：运行时编译执行 Scala 脚本，支持 UDF、自定义 HDFS 写入等 SQL 无法完成的逻辑。

## 编译

```bash
cd spark-task-runner
mvn clean package -DskipTests
```

打包后在 `target/spark-task-runner-1.0.0.jar`。

## 使用

### SQL 任务

文件路径支持本地路径和 HDFS 路径。每条语句以分号 `;` 结尾，支持单行注释 `--` 和块注释 `/* */`。

### Scala 任务

文件路径支持本地路径和 HDFS 路径。脚本被包进 `object Script { def main(args: Array[String]): Unit = { ... } }` 中在 driver 端运行时编译执行。脚本内通过 `SparkSession.builder().getOrCreate()` 获取 SparkSession，例如：

```scala
val spark = SparkSession.builder().getOrCreate()
spark.udf.register("my_udf", (x: Int) => x + 1)
val result = spark.sql("""SELECT my_udf(col) FROM t""")
result.coalesce(1).write.mode("overwrite").csv("hdfs://mycluster/out")
```

## Client 模式运行说明（Crown Cluster）

client 模式下 driver 运行在提交节点，使用本地 `$SPARK_HOME/jars/*`（without-hadoop 版，233 个 jar）。该目录缺少 `spark-hive` 与 Hive 2.3 client jar——它们只存在于 HDFS 的 `spark-jars`，而 `spark.yarn.jars` 只把 jar 提供给 YARN 容器（AM/executor），不提供给 client 模式的 driver。因此在 client 模式下 `spark.sql()` 会报 `ClassNotFoundException: org.apache.spark.sql.hive.HiveSessionStateBuilder`。

解决方法：给 driver 补上这 21 个 delta jar，两种写法等价：

- 每次提交时加 `--driver-class-path "/opt/local-spark-jars/spark-driver-extra/*"`；或
- 写进 `spark-defaults.conf` 一劳永逸：`spark.driver.extraClassPath /opt/local-spark-jars/spark-driver-extra/*`

- 命令行写法要用双引号包住 `*`，防止 shell 展开通配符，交给 JVM 按通配符加载该目录下的 jar。
- `spark-driver-extra/` 下是 21 个指向 hadoop3 bundle 的相对软链接，只补 `spark-hive` + Hive 2.3 client 等 delta jar；故意不含 Hadoop jar，以免与集群自带的 Hadoop 3.4.2 冲突。
- 写进 `spark-defaults.conf` 是安全的：cluster 模式下 AM 的 jar 来自 HDFS 的 `spark-jars`（`__spark_libs__`），不依赖这个本地目录；AM 即便落到 crown2/3、该目录缺失，JVM 也会静默跳过（通配符展开为空，不报错，已实测）。唯一限制：client 模式须在 crown1 提交——driver 在提交节点运行，需要该目录存在。

## 示例

四种组合（SQL/Scala × cluster/client）：

```bash
# 1. SQL · cluster 模式 · HDFS 路径
spark-4.1.2-bin-without-hadoop/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.spark.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar \
  hdfs://mycluster/myfiles/select.sql

# 2. SQL · client 模式 · 本地文件（--files 分发，参数传裸文件名）
spark-4.1.2-bin-without-hadoop/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-class-path "/opt/local-spark-jars/spark-driver-extra/*" \
  --class com.example.spark.SparkSqlRunner \
  --files /opt/select.sql \
  /opt/spark-task-runner-1.0.0.jar \
  select.sql

# 3. Scala · cluster 模式 · 本地文件（--files 分发，参数传裸文件名）
spark-4.1.2-bin-without-hadoop/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.spark.SparkScalaRunner \
  --files /opt/spark-scala.scala \
  /opt/spark-task-runner-1.0.0.jar \
  spark-scala.scala

# 4. Scala · client 模式 · HDFS 路径
spark-4.1.2-bin-without-hadoop/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-class-path "/opt/local-spark-jars/spark-driver-extra/*" \
  --class com.example.spark.SparkScalaRunner \
  /opt/spark-task-runner-1.0.0.jar \
  hdfs://mycluster/myfiles/spark-scala.scala
```

## SQL 文件格式

```sql
-- 创建临时视图
create or replace temporary view my_view (...) using csv options (
  path '/myfiles/data.csv',
  delimiter ',',
  header 'true'
);

-- 插入数据
INSERT OVERWRITE TABLE test1.my_table
SELECT * FROM my_view;
```

## 文件说明

- `src/main/java/com/example/spark/SparkSqlRunner.java`：SQL 任务主程序
- `src/main/scala/com/example/spark/SparkScalaRunner.scala`：Scala 任务主程序（运行时编译）
- `pom.xml`：Maven 配置

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

```bash
spark-submit --class com.example.spark.SparkSqlRunner \
    target/spark-task-runner-1.0.0.jar \
    <sql文件路径>
```

文件路径支持本地路径和 HDFS 路径。每条语句以分号 `;` 结尾，支持单行注释 `--` 和块注释 `/* */`。

### Scala 任务

```bash
spark-submit --class com.example.spark.SparkScalaRunner \
    target/spark-task-runner-1.0.0.jar \
    <scala脚本路径> [脚本参数...]
```

文件路径支持本地路径和 HDFS 路径。脚本被包进 `object Script { def main(args: Array[String]): Unit = { ... } }` 中在 driver 端运行时编译执行。脚本内通过 `SparkSession.builder().getOrCreate()` 获取 SparkSession，例如：

```scala
val spark = SparkSession.builder().getOrCreate()
spark.udf.register("my_udf", (x: Int) => x + 1)
val result = spark.sql("""SELECT my_udf(col) FROM t""")
result.coalesce(1).write.mode("overwrite").csv("hdfs://mycluster/out")
```

## 示例

```bash
# SQL（HDFS 文件）
spark-submit --class com.example.spark.SparkSqlRunner \
    target/spark-task-runner-1.0.0.jar \
    hdfs://mycluster/myfiles/import.sql

# Scala（HDFS 文件）
spark-submit --class com.example.spark.SparkScalaRunner \
    target/spark-task-runner-1.0.0.jar \
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

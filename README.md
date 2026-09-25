# spark-task-runner

通用 Spark 任务执行器，支持 SQL 和 Scala 两种任务，通过 `--class` 选择入口：

- `io.github.yanjin24.sparktaskrunner.SparkSqlRunner`：执行 SQL 文件，按分号拆分语句依次执行；SELECT/WITH 查询语句会打印查询结果。
- `io.github.yanjin24.sparktaskrunner.SparkScalaRunner`：运行时编译执行 Scala 脚本，支持 UDF、自定义 HDFS 写入等 SQL 无法完成的逻辑。

## 编译

```bash
cd spark-task-runner
mvn clean package -DskipTests
```

打包后在 `target/spark-task-runner-1.0.0.jar`。

## 使用

### SQL 任务

文件路径支持本地路径和 HDFS 路径。支持 `set` 会话配置；`--` 单行注释与 `/* */` 块注释在拆分时剥离，`/*+ ... */` hint 注释原样保留给 Spark，例如：

```sql
-- 会话级配置（仅影响本次运行）
set spark.sql.sources.partitionOverwriteMode=dynamic;

-- 创建临时视图（USING csv 直接读文件，无需建表）
CREATE OR REPLACE TEMPORARY VIEW my_view (id BIGINT, name STRING, dt STRING)
USING csv OPTIONS (
  path '/myfiles/data.csv',
  delimiter ',',
  header 'true'
);

-- 查询语句直接打印结果
SELECT name, count(*) AS cnt
FROM my_view
GROUP BY name
ORDER BY cnt DESC;

-- 动态分区覆盖写入：只覆盖数据涉及的分区（依赖开头的 set）
INSERT OVERWRITE TABLE mydb.my_table PARTITION (dt)
SELECT id, name, dt FROM my_view;
```

### Scala 任务

文件路径支持本地路径和 HDFS 路径。脚本被包进 `object Script { def main(args: Array[String]): Unit = { ... } }` 中在 driver 端运行时编译执行。脚本内通过 `SparkSession.builder().getOrCreate()` 获取 SparkSession（取回的是 runner 已创建的同一个会话，不会新建），例如：

```scala
import org.apache.spark.sql.SparkSession

// class / def 定义放在首次使用之前，不能后置
case class Score(name: String, subject: String, score: Int)

def topLabel(score: Int): String = if (score >= 90) "A" else "B"

val spark = SparkSession.builder().appName("成绩单任务").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")   // SQL 配置可运行时设置

spark.udf.register("label", (score: Int) => topLabel(score))
val df = spark.createDataFrame(Seq(
  Score("alice", "math", 95),
  Score("bob", "math", 82)
))
df.createOrReplaceTempView("scores")

spark.sql("SELECT name, subject, label(score) AS grade FROM scores").show()

df.coalesce(1).write.mode("overwrite").csv("hdfs://mycluster/out")
```

#### 脚本与普通 Scala 文件的差异

与工程里 `object` 包裹的文件相比，脚本有几个差异：

- **定义必须先于使用**。方法体内的 class / def / val 都是局部定义，前向引用一旦跨过中间的 val 定义就是编译错误。从 `object` 包裹的工程文件转成脚本时，把 class 和工具函数挪到脚本开头（import 之后）即可。
- 脚本里不要写 `package` 声明——它会落入方法体内，直接语法错误。

#### appName 与配置的生效规则

appName：

- **优先级**：`--name`（或 `--conf spark.app.name`）> 脚本中的 `.appName("...")` > spark-submit 默认（主类名）。
- **可见范围**：脚本 appName 生效于 `spark.sparkContext.appName` 与 Spark UI / History Server 的应用名，client 模式下也会成为 YARN 应用名；**cluster 模式的 YARN 应用名在提交时刻就已定死**（早于 driver 运行），只受 `--name` 影响——要让它显示特定名字，提交时加 `--name`。

配置：

- **运行时可改**：SQL 配置，如 `spark.sql.adaptive.*`、`spark.sql.shuffle.partitions`——`spark.conf.set(...)` 或 builder 的 `.config(...)` 都生效。
- **必须在提交时给**：`spark.executor.memory` / `--executor-cores` / `--num-executors`、`spark.serializer` 等 SparkContext 创建期定死的配置——runner 在脚本运行前就已创建会话，脚本内设置无效。

## Client 模式运行说明

client 模式下 driver 运行在提交节点，使用本地 `$SPARK_HOME/jars/*`（without-hadoop 版，233 个 jar）。该目录缺少 `spark-hive` 与 Hive 2.3 client jar——它们只存在于 HDFS 的 `spark-jars`，而 `spark.yarn.jars` 只把 jar 提供给 YARN 容器（AM/executor），不提供给 client 模式的 driver。因此在 client 模式下 `spark.sql()` 会报 `ClassNotFoundException: org.apache.spark.sql.hive.HiveSessionStateBuilder`。

解决方法：给 driver 补上这 21 个 delta jar（完整清单与两个用途见 [spark-jars.md](spark-jars.md)），两种写法等价：

- 每次提交时加 `--driver-class-path "/opt/local-spark-jars/spark-driver-extra/*"`；
- 写进 `spark-defaults.conf` 一劳永逸：`spark.driver.extraClassPath /opt/local-spark-jars/spark-driver-extra/*`。

两点说明：

- 写进 `spark-defaults.conf` 是安全的：cluster 模式下 AM 的 jar 来自 HDFS 的 `spark-jars`（`__spark_libs__`），不依赖这个本地目录；AM 即便调度到未部署该目录的节点，JVM 也会静默跳过（通配符展开为空，不报错，已实测）。唯一限制：client 模式须在已部署该目录的节点提交——driver 在提交节点运行，需要该目录存在。
- 命令行写法要用双引号包住 `*`，防止 shell 展开通配符，交给 JVM 按通配符加载该目录下的 jar。

## 示例

四种组合（SQL/Scala × cluster/client）：

```bash
# 1. SQL · cluster 模式 · HDFS 路径
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar \
  hdfs://mycluster/myfiles/select.sql

# 2. SQL · client 模式 · 本地文件（本地/HDFS 路径直接作参数）
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-class-path "/opt/local-spark-jars/spark-driver-extra/*" \
  --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar \
  /opt/select.sql

# 3. Scala · cluster 模式 · 本地文件（--files 分发，参数传裸文件名）
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class io.github.yanjin24.sparktaskrunner.SparkScalaRunner \
  --files /opt/spark-scala.scala \
  /opt/spark-task-runner-1.0.0.jar \
  spark-scala.scala

# 4. Scala · client 模式 · HDFS 路径
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-class-path "/opt/local-spark-jars/spark-driver-extra/*" \
  --class io.github.yanjin24.sparktaskrunner.SparkScalaRunner \
  /opt/spark-task-runner-1.0.0.jar \
  hdfs://mycluster/myfiles/spark-scala.scala
```

操作 Iceberg 表的提交写法（额外 jar、catalog 配置、模式选择）见 [iceberg.md](iceberg.md)。

## 文件说明

- `src/main/java/io/github/yanjin24/sparktaskrunner/SparkSqlRunner.java`：SQL 任务主程序
- `src/main/java/io/github/yanjin24/sparktaskrunner/ScriptFileReader.java`：脚本/SQL 文件读取工具（本地与 HDFS 路径，UTF-8/GBK 编码识别、BOM 处理）
- `src/main/scala/io/github/yanjin24/sparktaskrunner/SparkScalaRunner.scala`：Scala 任务主程序（运行时编译）
- `pom.xml`：Maven 配置

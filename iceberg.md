# Iceberg 表操作

通过将内置 catalog（`spark_catalog`）替换为 Iceberg 的 `SparkSessionCatalog`，Iceberg 表与普通 Hive 表在 SQL 中统一使用二段式名 `库名.表名`：Iceberg 表由 Iceberg 处理，其余表自动回退到内置 catalog。若不注册，二段式引用 Iceberg 表会报 `ClassNotFoundException: org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`。

## 前提配置

jar 构成：`iceberg-spark-runtime-4.1_2.13-1.11.0.jar`（Iceberg 的 Spark 支持，不含 Hive client 类，下文简称 runtime jar）与 Hive 4.1 的 3 个 metastore client jar——hive-metastore / hive-standalone-metastore-common / hive-storage-api（本地目录内为符号链接，下文简称 metastore jar）。两处存放，按用途取用：

- 提交节点本地 `/opt/local-spark-jars/`（目录用途见 [spark-jars.md](spark-jars.md)）：client 模式 `--driver-class-path` 使用；
- HDFS `/spark/iceberg-jars/`：4 个 jar 的一次性上传位置，由 spark-defaults.conf 的 `spark.jars` 提供给该节点所有 spark 启动。

本文示例命令均以 spark-defaults.conf 已配置如下 `spark.jars` 为前提，不再出现 `--jars`：

```properties
spark.jars  hdfs://mycluster/spark/iceberg-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar,hdfs://mycluster/spark/iceberg-jars/hive-metastore-4.1.0.jar,hdfs://mycluster/spark/iceberg-jars/hive-standalone-metastore-common-4.1.0.jar,hdfs://mycluster/spark/iceberg-jars/hive-storage-api-4.1.0.jar
```

用 HDFS 稳定路径而非本地路径的原因：本地路径每次提交都会先上传到 `.sparkStaging/<appId>`，YARN 再按资源 URL 计缓存、每个应用在每个节点各本地化一份；稳定路径不中转，各节点所有应用共用一份公共缓存（HDFS 权限链全可读时全用户共享）。

`spark.jars` 的 jar 由 child classloader 加载，主 classpath（`--driver-class-path`）在父类加载器上，默认 parent-first 委派下同名类以主 classpath 为准。两种模式的差异都源于此：

- **cluster 模式**：容器直接引用 HDFS 路径，4 个 jar 全部生效；配合 `spark.driver.userClassPathFirst=true` 改为 child-first 委派，使这批 jar 优先于 `spark.yarn.jars` 里的 2.x 版 metastore jar。
- **client 模式（含 spark-sql CLI）**：driver 每次启动先将 4 个 jar 下载到本地临时目录（含已被 `--driver-class-path` 覆盖的 metastore jar，属冗余下载），退出自动清理；真正生效的只有 runtime jar。前置 metastore jar 的要求不变，CLI 查普通表的既有写法也不受全局配置影响（child 中的 4.1 输给 CLI 主 classpath 的 2.3）。

## 提交命令

client 模式——Iceberg 的 HiveCatalog 从 driver classpath 加载 metastore client，需将 metastore jar 放在 classpath 前部，优先于 2.3 版加载：

```bash
$SPARK_HOME/bin/spark-submit \
  --master yarn --deploy-mode client \
  --driver-class-path "/opt/local-spark-jars/iceberg-hive-4.1/*:/opt/local-spark-jars/spark-driver-extra/*" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://<metastore-host>:9083 \
  --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar /opt/select-iceberg.sql
```

cluster 模式——`spark.driver.userClassPathFirst=true` 让 `spark.jars` 提供的 metastore jar 压过 `spark.yarn.jars` 里的 2.x 版（child-first 委派）；SQL 脚本若以 HDFS 路径作为程序参数，无需 `--files`：

```bash
$SPARK_HOME/bin/spark-submit \
  --master yarn --deploy-mode cluster \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://<metastore-host>:9083 \
  --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar hdfs://mycluster/myfiles/select-iceberg.sql
```

说明：

- 三行 `--conf` catalog 配置可写入 `spark-defaults.conf`，此后提交命令可省去。
- SQL 脚本仍可走 `--files 本地路径` + 裸文件名参数（cluster 模式），每次提交本地化小文件代价可忽略。
- 两个示例以 SparkSqlRunner + SQL 脚本为例；SparkScalaRunner + Scala 脚本同理，脚本内 `spark.sql()` 同样直接使用二段式表名——jar 与 catalog 配置只作用于 SparkSession，与任务入口无关。

## SQL 示例

示例中 Iceberg 表在 `iceberg_db` 库，普通 Hive 表在 `mydb` 库：

```sql
-- 建表 / CTAS（源表与目标表直接二段式）
CREATE TABLE iceberg_db.new_table
USING iceberg
AS SELECT ... FROM iceberg_db.src_table;

-- 查询（Iceberg 表与普通 Hive 表关联使用）
select a,b,c
from mydb.textfile_table
join iceberg_db.iceberg_table on m = n;

-- 增删改
INSERT INTO iceberg_db.t VALUES (1, 'a'), (2, 'b');
UPDATE iceberg_db.t SET col = 1 WHERE id = 1;
DELETE FROM iceberg_db.t WHERE id = 1;
```

## 模式选择

- 只操作 Iceberg 表、只查普通 Hive 表、或两者混用：client / cluster 两种模式的示例写法均适用（混用、以及前置 metastore jar 查 TEXTFILE 表）。
- 只查普通表用不到 Iceberg 相关配置，可采用更简形态：[README](README.md) 「示例」一节的四种组合。

> 以上结论仅适用于 spark-submit 提交方式。spark-sql CLI 将 `iceberg-hive-4.1/*` 前置于 classpath 查原生格式（TEXTFILE 等）普通表时会报 `NoSuchMethodError`：CLI 会话持有 hive-exec-2.3 的 CliSessionState，查普通表时其授权初始化经 `Hive.getMSC()` 调 2.3 版签名的 `RetryingMetaStoreClient.getProxy`，而实际加载到的是前置的 4.1 版类，签名不匹配；spark-submit 的 driver 没有 CLI 式 SessionState，不会走到 `Hive.getMSC()`，故 client 模式用同样的平铺前置 classpath 也不受影响。CLI 会话查普通表须去掉 metastore jar。

# Iceberg 表操作

通过将内置 catalog（`spark_catalog`）替换为 Iceberg 的 `SparkSessionCatalog`，Iceberg 表与普通 Hive 表在 SQL 中统一使用二段式名 `库名.表名`：Iceberg 表由 Iceberg 处理，其余表自动回退到内置 catalog。若不注册，二段式引用 Iceberg 表会报 `ClassNotFoundException: org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`。

所需 jar 均在提交节点 `/opt/local-spark-jars/`（目录用途见 [spark-jars.md](spark-jars.md)）：

- `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`：Iceberg 的 Spark 支持，不含 Hive client 类；
- `iceberg-hive-4.1/`：Hive 4.1 的 3 个 metastore client jar——hive-metastore / hive-standalone-metastore-common / hive-storage-api（目录内为符号链接，下文简称 metastore jar）。

## 提交命令

client 模式——Iceberg 的 HiveCatalog 从 driver classpath 加载 metastore client，需将 metastore jar 放在 classpath 前部，优先于 2.3 版加载：

```bash
$SPARK_HOME/bin/spark-submit \
  --master yarn --deploy-mode client \
  --driver-class-path "/opt/local-spark-jars/iceberg-hive-4.1/*:/opt/local-spark-jars/spark-driver-extra/*" \
  --jars /opt/local-spark-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://<metastore-host>:9083 \
  --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar /opt/select-iceberg.sql
```

cluster 模式——`spark.driver.userClassPathFirst` 使 metastore jar 在 user classloader 中优先于容器自带的 spark-jars 中 2.x 版 metastore jar 加载：

```bash
$SPARK_HOME/bin/spark-submit \
  --master yarn --deploy-mode cluster \
  --conf spark.driver.userClassPathFirst=true \
  --jars /opt/local-spark-jars/iceberg-hive-4.1/*.jar,/opt/local-spark-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://<metastore-host>:9083 \
  --files /opt/select-iceberg.sql \
  --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner \
  /opt/spark-task-runner-1.0.0.jar select-iceberg.sql
```

说明：

- `--jars` 中的 `*.jar` 由 SparkSubmit 逐项经 `FileSystem.globStatus` 展开为实际文件，可直接写在逗号列表内；runtime 与 metastore jar 无重叠类，先后顺序无关。
- 三行 `--conf` catalog 配置可写入 `spark-defaults.conf`，此后提交命令可省去。

## SQL 示例

示例中 Iceberg 表在 `iceberg_db` 库，普通 Hive 表在 `mydb` 库：

```sql
-- 建表 / CTAS（源表与目标表直接二段式）
CREATE TABLE iceberg_db.new_table
USING iceberg TBLPROPERTIES ('write.parquet.compression-codec'='zstd')
AS SELECT ... FROM iceberg_db.src_table;

-- 查询（Iceberg 表与普通 Hive 表写法一致）
SELECT count(1) FROM iceberg_db.t;
SELECT count(1) FROM mydb.plain_table;

-- 增删改（Iceberg 行级 ACID，无需额外扩展）
INSERT INTO iceberg_db.t VALUES (1, 'a'), (2, 'b');
UPDATE iceberg_db.t SET col = 1 WHERE id = 1;
DELETE FROM iceberg_db.t WHERE id = 1;
```

## 模式选择

- 只操作 Iceberg 表：两种模式皆可。
- 混用普通 Hive 表与 Iceberg 表（建表与增删改查）：两种模式均实测通过。只查普通表的 client 提交见 [README](README.md) 的「Client 模式运行说明」。

> 以上结论仅适用于 spark-submit 提交方式。spark-sql CLI 将 `iceberg-hive-4.1/*` 前置于 classpath 查原生格式（TEXTFILE 等）普通表时会报 `NoSuchMethodError`：hive-exec-2.3 的 SessionState 授权初始化经 `Hive.getMSC()` 调 2.3 版签名的 `RetryingMetaStoreClient.getProxy`，而实际加载到的是前置的 4.1 版类，签名不匹配。CLI 会话查普通表须去掉 metastore jar。

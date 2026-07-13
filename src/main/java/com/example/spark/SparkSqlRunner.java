package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 通用 Spark SQL 执行器。
 * 读取 SQL 文件，按分号拆分语句，依次执行。
 *
 * 用法: spark-submit --class com.example.spark.SparkSqlRunner
 * spark-csv-import-1.0.0.jar <sql文件路径>
 */
public class SparkSqlRunner {
    /** 查询结果最多展示的行数。 */
    private static final int SHOW_ROWS = 500;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("用法: spark-submit --class com.example.spark.SparkSqlRunner <jar> <sql文件路径>");
            System.exit(1);
        }

        String sqlFilePath = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Runner")
                .getOrCreate();

        try {
            String content = readSqlFile(sqlFilePath);
            List<String> statements = splitStatements(content);

            System.out.println("SQL文件: " + sqlFilePath);
            System.out.println("共 " + statements.size() + " 条语句\n");

            for (int i = 0; i < statements.size(); i++) {
                String sql = statements.get(i);
                System.out.println("--- 执行第 " + (i + 1) + " 条语句 ---");
                if (isQuery(sql)) {
                    // 查询语句：捕获结果集并展示
                    Dataset<Row> result = spark.sql(sql);
                    System.out.println("查询结果：");
                    result.show(SHOW_ROWS, false);
                } else {
                    spark.sql(sql);
                }
                System.out.println("第 " + (i + 1) + " 条语句执行完成\n");
            }

            System.out.println("全部语句执行完成！");
        } catch (IOException e) {
            System.err.println("读取SQL文件失败: " + sqlFilePath);
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("SQL执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * 读取 SQL 文件，支持本地路径和 HDFS URI。
     * 本地路径: /opt/spark-sql.sql
     * HDFS URI: hdfs://mycluster/spark-sql.sql
     */
    private static String readSqlFile(String path) throws IOException {
        byte[] bytes;
        if (path.startsWith("hdfs://")) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(path), conf);
            try (var is = fs.open(new Path(path))) {
                bytes = is.readAllBytes();
            }
        } else {
            bytes = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(path));
        }
        return decodeBytes(bytes);
    }

    /**
     * 解码文件字节：先按 UTF-8 严格解码，失败则回退 GBK。
     * 让本地路径与 HDFS 路径行为一致，并兼容 Windows 下 GBK 编码的脚本/SQL 文件。
     */
    private static String decodeBytes(byte[] bytes) {
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return new String(bytes, Charset.forName("GBK"));
        }
    }

    /**
     * 按分号拆分 SQL 语句，忽略空语句和注释行。
     * 支持单行注释 (--) 和块注释。
     */
    static List<String> splitStatements(String content) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            char next = i + 1 < content.length() ? content.charAt(i + 1) : 0;

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append(c);
                }
                continue;
            }

            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++; // skip '/'
                }
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '-' && next == '-') {
                    inLineComment = true;
                    continue;
                }
                if (c == '/' && next == '*') {
                    inBlockComment = true;
                    i++; // skip '*'
                    continue;
                }
            }

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            }

            if (c == ';' && !inSingleQuote && !inDoubleQuote) {
                addStatement(statements, current);
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        // 末尾没有分号的语句也执行
        addStatement(statements, current);
        return statements;
    }

    private static void addStatement(List<String> statements, StringBuilder sb) {
        String trimmed = sb.toString().trim();
        if (!trimmed.isEmpty()) {
            statements.add(trimmed);
        }
    }

    /**
     * 判断语句是否为查询语句（SELECT / WITH 开头）。
     * 查询语句会捕获结果集并展示；其他语句（INSERT、CREATE 等）只执行不展示。
     */
    private static boolean isQuery(String sql) {
        String upper = sql.trim().toUpperCase();
        return startsWithKeyword(upper, "SELECT") || startsWithKeyword(upper, "WITH");
    }

    private static boolean startsWithKeyword(String upper, String keyword) {
        if (!upper.startsWith(keyword)) {
            return false;
        }
        if (upper.length() == keyword.length()) {
            return true;
        }
        char next = upper.charAt(keyword.length());
        return Character.isWhitespace(next) || next == '(';
    }
}
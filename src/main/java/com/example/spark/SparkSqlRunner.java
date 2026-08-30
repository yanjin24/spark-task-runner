package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通用 Spark SQL 执行器。
 * 读取 SQL 文件，按分号拆分语句，依次执行。
 *
 * 用法: spark-submit --class com.example.spark.SparkSqlRunner
 * spark-task-runner-1.0.0.jar <sql文件路径>
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
            // HDFS 读取走 Spark 的 hadoopConfiguration，继承 --conf / Kerberos 等配置
            String content = ScriptFileReader.read(sqlFilePath, spark.sparkContext().hadoopConfiguration());
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
     * 按分号拆分 SQL 语句，忽略空语句和注释行。
     * 支持单行注释 (--)、块注释、单引号/双引号字符串以及反引号标识符（其内可含分号）。
     */
    static List<String> splitStatements(String content) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBacktick = false;

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

            if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
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

            if (c == '\'' && !inDoubleQuote && !inBacktick) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote && !inBacktick) {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '`' && !inSingleQuote && !inDoubleQuote) {
                inBacktick = !inBacktick;
            }

            if (c == ';' && !inSingleQuote && !inDoubleQuote && !inBacktick) {
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

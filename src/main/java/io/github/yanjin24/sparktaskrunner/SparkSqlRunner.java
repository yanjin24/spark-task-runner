package io.github.yanjin24.sparktaskrunner;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkSqlRunner {

    private static final int SHOW_ROWS = 500;

    /** 未显式给名时使用的应用名。 */
    private static final String DEFAULT_APP_NAME = "Spark SQL Runner";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("用法: spark-submit --class io.github.yanjin24.sparktaskrunner.SparkSqlRunner <jar> <sql文件路径>");
            System.exit(1);
        }

        String sqlFilePath = args[0];

        // appName：--name / --conf spark.app.name > 本类固定名。
        // builder 的 appName 会覆盖提交时的 spark.app.name，故仅在未显式给名时才设置；
        SparkSession.Builder builder = SparkSession.builder();
        String submitName = new SparkConf().get("spark.app.name", null);
        if (submitName == null || submitName.equals(SparkSqlRunner.class.getName())) {
            builder.appName(DEFAULT_APP_NAME);
        }
        SparkSession spark = builder.getOrCreate();
        System.out.println("appName: " + spark.sparkContext().appName());

        try {
            String content = ScriptFileReader.read(sqlFilePath, spark.sparkContext().hadoopConfiguration());
            List<String> statements = splitStatements(content);

            System.out.println("SQL文件: " + sqlFilePath);
            System.out.println("共 " + statements.size() + " 条语句\n");

            for (int i = 0; i < statements.size(); i++) {
                String sql = statements.get(i);
                System.out.println("--- 执行第 " + (i + 1) + " 条语句 ---");
                // 让 Spark UI SQL 页的 Description 显示语句原文，与 spark-sql CLI 一致
                spark.sparkContext().setCallSite(sql);
                if (isQuery(sql)) {
                    Dataset<Row> result = spark.sql(sql);
                    System.out.println("查询结果：");
                    result.show(SHOW_ROWS, false);
                } else {
                    spark.sql(sql);
                }
                spark.sparkContext().clearCallSite();
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

    /** 按分号拆分 SQL 语句，支持单行/块注释、单引号/双引号字符串与反引号标识符（其内可含分号）；hint 注释原样保留。 */
    static List<String> splitStatements(String content) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean keepBlockComment = false;
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
                if (keepBlockComment) {
                    current.append(c);
                }
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    if (keepBlockComment) {
                        current.append(next);
                    }
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
                    // /*+ 开头的是 hint，原样保留给 Spark 解析
                    keepBlockComment = i + 2 < content.length() && content.charAt(i + 2) == '+';
                    if (keepBlockComment) {
                        current.append(c).append(next);
                    }
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

    /** 是否为查询语句（SELECT / WITH 开头）。 */
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

package io.github.yanjin24.sparktaskrunner;

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

/**
 * SQL/脚本文件读取工具，供 SparkSqlRunner 与 SparkScalaRunner 共用。
 */
final class ScriptFileReader {
    private ScriptFileReader() {
    }

    /**
     * 读取文件内容为字符串，支持本地路径和 HDFS URI。
     * HDFS 读取使用传入的 Hadoop Configuration（通常传 spark.sparkContext().hadoopConfiguration()），
     * 以继承 Spark 侧的 --conf、Kerberos 等配置，而不是用裸 new Configuration()。
     */
    static String read(String path, Configuration hadoopConf) throws IOException {
        byte[] bytes;
        if (path.startsWith("hdfs://")) {
            FileSystem fs = FileSystem.get(URI.create(path), hadoopConf);
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
     * UTF-8 严格解码成功时剥离开头的 BOM (U+FEFF)，避免 BOM 混入首条语句导致解析失败。
     */
    static String decodeBytes(byte[] bytes) {
        String text;
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            text = decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            text = new String(bytes, Charset.forName("GBK"));
        }
        if (!text.isEmpty() && text.charAt(0) == 0xFEFF) {
            text = text.substring(1);
        }
        return text;
    }
}

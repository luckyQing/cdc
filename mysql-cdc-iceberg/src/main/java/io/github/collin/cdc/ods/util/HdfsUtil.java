package io.github.collin.cdc.ods.util;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HdfsUtil {

    /**
     * 写文件到hdfs（覆盖）
     *
     * @param content
     * @param cacheDir
     * @param fileName
     * @throws IOException
     */
    public static void overwrite(String content, String cacheDir, String fileName) throws IOException {
        TextOutputFormat<String> format = new TextOutputFormat<>(new Path(cacheDir, fileName));
        format.setCharsetName(StandardCharsets.UTF_8.name());
        format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        try {
            format.open(1, 1);
            format.writeRecord(content);
        } finally {
            format.close();
        }
    }

}
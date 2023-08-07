package io.github.collin.cdc.common.util;

import org.springframework.core.io.ClassPathResource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;

import java.io.IOException;
import java.io.InputStream;

/**
 * yaml文件工具类
 *
 * @author collin
 * @date 2023-05-06
 */
public class YamlUtil {

    /**
     * 从yaml文件去读配置信息
     *
     * @param yamlPath
     * @return
     */
    public static <T> T readYaml(String yamlPath, Class<T> type) throws IOException {
        // 获取yaml文件路径
        ClassPathResource resource = new ClassPathResource(yamlPath);
        // 创建Yaml对象
        Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);
        Yaml yaml = new Yaml(representer);
        // 读取yaml文件信息
        T yamlProperties = null;
        try (InputStream yamlInputStream = resource.getInputStream()) {
            yamlProperties = yaml.loadAs(yamlInputStream, type);
        }

        return yamlProperties;
    }

}

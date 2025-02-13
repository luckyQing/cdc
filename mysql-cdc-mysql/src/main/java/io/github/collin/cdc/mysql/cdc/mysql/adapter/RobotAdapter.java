package io.github.collin.cdc.mysql.cdc.mysql.adapter;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import io.github.collin.cdc.common.properties.ProxyProperties;
import io.github.collin.cdc.common.properties.RobotProperties;
import io.github.collin.cdc.common.util.JacksonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * 企业微信通知
 *
 * @author collin
 * @date 2023-04-24
 */
@Slf4j
@RequiredArgsConstructor
public class RobotAdapter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ProxyProperties proxyProperties;
    private final RobotProperties robotProperties;

    /**
     * 接收到ddl后企业微信通知
     *
     * @param targetDbName
     * @param targetTable
     * @param ddlSql
     */
    public void noticeAfterReceiveDdl(String targetDbName, String targetTable, String ddlSql) {
        String url = robotProperties.getUrl();
        try {
            String showDdlSql = ddlSql.replaceAll("\n", "\\\\n");
            // （参数：目标库名、目标表名、ddl语句）
            String msg = String.format(robotProperties.getMessageTemplate(), targetDbName, targetTable, showDdlSql);

            HttpRequest httpRequest = HttpUtil.createPost(url).body(msg.getBytes(StandardCharsets.UTF_8)).setConnectionTimeout(3000).setReadTimeout(3000);

            if (proxyProperties != null && StringUtils.isNotBlank(proxyProperties.getHost()) && proxyProperties.getPort() != null) {
                httpRequest.setHttpProxy(proxyProperties.getHost(), proxyProperties.getPort());
            }

            httpRequest.execute(true);
        } catch (Exception e) {
            log.error("ddl sync notice fail|proxy={}, url={}, targetDbName={}, targetTable={}, ddlSql={}", JacksonUtil.toJson(proxyProperties), url,
                    targetDbName, targetTable, ddlSql, e);
        }
    }

}
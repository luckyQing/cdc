package io.github.collin.cdc.mysql.cdc.common.adapter;

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

    private final ProxyProperties proxy;
    private final RobotProperties ddlProperties;
    private final RobotProperties deleteProperties;

    /**
     * 接收到ddl后企业微信通知
     *
     * @param applicationId
     * @param jobId
     * @param sourceDdlSql
     * @param sourceOffset
     * @param targetDbName
     * @param targetTable
     * @param targetDdl
     */
    public void noticeAfterReceiveDdl(String applicationId, String jobId, String sourceDdlSql, String sourceOffset,
                                      String targetDbName, String targetTable, String targetDdl) {
        String url = ddlProperties.getUrl();
        try {
            String sourceOffsetStr = null;
            if (sourceOffset != null) {
                sourceOffsetStr = JacksonUtil.toJson(sourceOffset);
                sourceOffsetStr = sourceOffsetStr.substring(1, sourceOffsetStr.length() - 1);
            }

            String showDdlSql = sourceDdlSql.replaceAll("\n", "\\\\n");
            // （参数：订单号、应用id、任务id、目标库名、目标表名、sourceOffset、ddl语句、arctic ddl）
            String msg = String.format(ddlProperties.getMessageTemplate(), applicationId, jobId, targetDbName, targetTable, sourceOffsetStr, showDdlSql, targetDdl);

            HttpRequest httpRequest = HttpUtil.createPost(url)
                    .body(msg.getBytes(StandardCharsets.UTF_8))
                    .setConnectionTimeout(3000)
                    .setReadTimeout(3000);

            if (proxy != null && StringUtils.isNotBlank(proxy.getHost()) && proxy.getPort() != null) {
                httpRequest.setHttpProxy(proxy.getHost(), proxy.getPort());
            }

            httpRequest.execute(true);
        } catch (Exception e) {
            log.error("ddl sync notice fail|proxy={}, url={}, targetDbName={}, targetTable={}, sourceOffset={}, sourceDdlSql={}", JacksonUtil.toJson(proxy), url,
                    targetDbName, targetTable, sourceOffset, sourceDdlSql, e);
        }
    }

    public void noticeAfterReceiveDelete(String application, String targetDbName, String targetTable, String json) {
        try {
            json = json.replaceAll("\\\"", "\\\\\"");
            String msg = String.format(deleteProperties.getMessageTemplate(), application, targetDbName, targetTable, json);

            HttpRequest httpRequest = HttpUtil.createPost(deleteProperties.getUrl())
                    .body(msg.getBytes(StandardCharsets.UTF_8))
                    .setConnectionTimeout(3000)
                    .setReadTimeout(3000);

            if (proxy != null && StringUtils.isNotBlank(proxy.getHost()) && proxy.getPort() != null) {
                httpRequest.setHttpProxy(proxy.getHost(), proxy.getPort());
            }

            httpRequest.execute(true);
        } catch (Exception e) {
            log.error("delete data notice fail|proxy={}, url={}, targetDbName={}, targetTable={}, ddlSql={}", JacksonUtil.toJson(proxy), deleteProperties.getUrl(),
                    targetDbName, targetTable, json, e);
        }
    }

}
package io.github.collin.cdc.mysql.cdc.iceberg.adapter;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import io.github.collin.cdc.common.properties.ProxyProperties;
import io.github.collin.cdc.common.properties.RobotProperties;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.mysql.cdc.iceberg.properties.MonitorProperties;
import io.github.collin.cdc.mysql.cdc.iceberg.util.DdlUtil;
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
    private final MonitorProperties monitorProperties;

    /**
     * 接收到ddl后企业微信通知
     *
     * @param applicationId
     * @param orderNo
     * @param jobId
     * @param targetDbName
     * @param targetTable
     * @param ddlSql
     * @param sourceOffset
     */
    public void noticeAfterReceiveDdl(String orderNo, String applicationId, String jobId, String targetDbName, String targetTable, String ddlSql, String sourceOffset) {
        RobotProperties ddlMonitor = monitorProperties.getDdl();
        String url = ddlMonitor.getUrl();
        try {
            String arcticDdl = DdlUtil.convertArcticSql(ddlSql, targetDbName, targetTable);
            String sourceOffsetStr = JacksonUtil.toJson(sourceOffset);
            sourceOffsetStr = sourceOffsetStr.substring(1, sourceOffsetStr.length() - 1);
            String showDdlSql = ddlSql.replaceAll("\n", "\\\\n");
            // （参数：订单号、应用id、任务id、目标库名、目标表名、sourceOffset、ddl语句、arctic ddl）
            String msg = String.format(ddlMonitor.getMessageTemplate(), orderNo, applicationId, jobId, targetDbName, targetTable, sourceOffsetStr, showDdlSql, arcticDdl);

            HttpRequest httpRequest = HttpUtil.createPost(url).body(msg.getBytes(StandardCharsets.UTF_8)).setConnectionTimeout(3000).setReadTimeout(3000);

            if (proxy != null && StringUtils.isNotBlank(proxy.getHost()) && proxy.getPort() != null) {
                httpRequest.setHttpProxy(proxy.getHost(), proxy.getPort());
            }

            httpRequest.execute(true);
        } catch (Exception e) {
            log.error("ddl sync notice fail|proxy={}, url={}, targetDbName={}, targetTable={}, sourceOffset={}, ddlSql={}", JacksonUtil.toJson(proxy), url,
                    targetDbName, targetTable, sourceOffset, ddlSql, e);
        }
    }

    /**
     * ddl同步后企业微信通知
     *
     * @param application
     * @param targetDbName
     * @param targetTable
     * @param ddlSql
     * @param success
     */
    public void noticeAfterSynced(String application, String targetDbName, String targetTable, String ddlSql, boolean success) {
        RobotProperties ddlSync = monitorProperties.getDdlSync();
        String url = ddlSync.getUrl();
        try {
            String showDdlSql = ddlSql.replaceAll("\n", "\\\\n");
            String result = success ? "<font color=\\\"info\\\">同步成功</font>" : "<font color=\\\"warning\\\">**同步失败**</font>";
            String msg = String.format(ddlSync.getMessageTemplate(), application, targetDbName, targetTable, showDdlSql, result);

            HttpRequest httpRequest = HttpUtil.createPost(url).body(msg.getBytes(StandardCharsets.UTF_8)).setConnectionTimeout(3000).setReadTimeout(3000);

            if (proxy != null && StringUtils.isNotBlank(proxy.getHost()) && proxy.getPort() != null) {
                httpRequest.setHttpProxy(proxy.getHost(), proxy.getPort());
            }

            httpRequest.execute(true);
        } catch (Exception e) {
            log.error("ddl sync notice fail|proxy={}, url={}, targetDbName={}, targetTable={}, ddlSql={}", JacksonUtil.toJson(proxy), url,
                    targetDbName, targetTable, ddlSql, e);
        }
    }

    public void noticeAfterReceiveDelete(String application, String targetDbName, String targetTable, String json) {
        RobotProperties deleteProperties = monitorProperties.getDelete();
        try {
            json = json.replaceAll("\\\"","\\\\\"");
            String msg = String.format(deleteProperties.getMessageTemplate(), application, targetDbName, targetTable, json);

            HttpRequest httpRequest = HttpUtil.createPost(deleteProperties.getUrl()).body(msg.getBytes(StandardCharsets.UTF_8)).setConnectionTimeout(3000).setReadTimeout(3000);

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
package com.bili.flink.vip;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * HBase Sink - 写入dim:dim_account表更新VIP信息
 * 更新字段：
 * - status:vip_type - VIP类型
 * - status:vip_type_name - VIP类型名称
 * - status:vip_expire - VIP到期时间
 * - etl:updated_at - 更新时间
 */
public class HBaseVipSinkFunction extends RichSinkFunction<VipEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseVipSinkFunction.class);
    private static final long serialVersionUID = 1L;

    private final String zkQuorum;
    private final String tableName;

    private transient Connection connection;
    private transient Table table;
    private transient DateTimeFormatter dateFormatter;

    // 列族和列名
    private static final byte[] CF_STATUS = Bytes.toBytes("status");
    private static final byte[] CF_ETL = Bytes.toBytes("etl");
    private static final byte[] COL_VIP_TYPE = Bytes.toBytes("vip_type");
    private static final byte[] COL_VIP_TYPE_NAME = Bytes.toBytes("vip_type_name");
    private static final byte[] COL_VIP_EXPIRE = Bytes.toBytes("vip_expire");
    private static final byte[] COL_UPDATED_AT = Bytes.toBytes("updated_at");
    private static final byte[] COL_DW_UPDATE_TIME = Bytes.toBytes("dw_update_time");

    public HBaseVipSinkFunction(String zkQuorum, String tableName) {
        this.zkQuorum = zkQuorum;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", zkQuorum);
        hbaseConfig.set("hbase.client.retries.number", "3");
        hbaseConfig.set("hbase.client.pause", "1000");

        connection = ConnectionFactory.createConnection(hbaseConfig);
        table = connection.getTable(TableName.valueOf(tableName));
        dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.of("Asia/Shanghai"));

        LOG.info("HBase connection established, table: {}", tableName);
    }

    @Override
    public void invoke(VipEvent event, Context context) throws Exception {
        if (event.getMid() == null) {
            LOG.warn("Skipping event with null mid: {}", event.getOrderNo());
            return;
        }

        VipEvent.VipProperties props = event.getProperties();
        if (props == null || props.getNewVipExpire() == null) {
            LOG.warn("Skipping event without vip expire info: {}", event.getOrderNo());
            return;
        }

        // RowKey: mid反转（与DimAccount.sql保持一致）
        String rowKey = new StringBuilder(String.valueOf(event.getMid())).reverse().toString();

        // 根据plan_id确定新的VIP类型
        int newVipType = determineVipType(props.getPlanId());
        String vipTypeName = getVipTypeName(newVipType);

        // VIP到期时间（秒级时间戳转为可读格式）
        String vipExpire = dateFormatter.format(Instant.ofEpochSecond(props.getNewVipExpire()));
        String updateTime = dateFormatter.format(Instant.now());

        // 构建Put操作
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(CF_STATUS, COL_VIP_TYPE, Bytes.toBytes(String.valueOf(newVipType)));
        put.addColumn(CF_STATUS, COL_VIP_TYPE_NAME, Bytes.toBytes(vipTypeName));
        put.addColumn(CF_STATUS, COL_VIP_EXPIRE, Bytes.toBytes(vipExpire));
        put.addColumn(CF_ETL, COL_UPDATED_AT, Bytes.toBytes(updateTime));
        put.addColumn(CF_ETL, COL_DW_UPDATE_TIME, Bytes.toBytes(updateTime));

        table.put(put);

        LOG.info("HBase updated: mid={}, rowKey={}, vipType={}, vipExpire={}, orderNo={}",
                event.getMid(), rowKey, newVipType, vipExpire, event.getOrderNo());
    }

    /**
     * 根据套餐ID确定VIP类型
     */
    private int determineVipType(String planId) {
        if (planId == null) return 2; // 默认年度大会员

        switch (planId) {
            case "monthly":
            case "auto_monthly":
                return 1; // 月度大会员
            case "yearly":
            case "auto_yearly":
            default:
                return 2; // 年度大会员
        }
    }

    /**
     * 获取VIP类型名称
     */
    private String getVipTypeName(int vipType) {
        switch (vipType) {
            case 0: return "普通用户";
            case 1: return "月度大会员";
            case 2: return "年度大会员";
            default: return "未知";
        }
    }

    @Override
    public void close() throws Exception {
        if (table != null) {
            table.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        LOG.info("HBase connection closed");
    }
}

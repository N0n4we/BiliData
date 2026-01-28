package com.bili.flink.vip;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * VIP订单处理Flink作业
 *
 * 数据流：
 * Kafka(app_event_vip) -> VipOrderProcessFunction(Redis缓存) -> HBaseSink(dim:dim_account)
 *
 * 处理逻辑：
 * 1. vip_create_order: 缓存订单到Redis（带TTL）
 * 2. vip_pay_success: 删除Redis缓存，写HBase更新VIP信息
 * 3. vip_pay_fail/vip_pay_cancel: 删除Redis缓存
 */
public class VipOrderProcessJob {

    private static final Logger LOG = LoggerFactory.getLogger(VipOrderProcessJob.class);

    public static void main(String[] args) throws Exception {
        // 配置参数（生产环境应从配置文件或命令行参数读取）
        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String kafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "app_event_vip");
        String kafkaGroupId = getEnvOrDefault("KAFKA_GROUP_ID", "flink-vip-order-process");

        String redisHost = getEnvOrDefault("REDIS_HOST", "redis");
        int redisPort = Integer.parseInt(getEnvOrDefault("REDIS_PORT", "6379"));
        String redisPassword = getEnvOrDefault("REDIS_PASSWORD", "");
        int orderTtlSeconds = Integer.parseInt(getEnvOrDefault("ORDER_TTL_SECONDS", "3600"));

        String hbaseZkQuorum = getEnvOrDefault("HBASE_ZK_QUORUM", "hbase-master:2181");
        String hbaseTable = getEnvOrDefault("HBASE_TABLE", "dim:dim_account");

        LOG.info("Starting VipOrderProcessJob with config:");
        LOG.info("  Kafka: {} / {} / {}", kafkaBootstrapServers, kafkaTopic, kafkaGroupId);
        LOG.info("  Redis: {}:{}, TTL={}s", redisHost, redisPort, orderTtlSeconds);
        LOG.info("  HBase: {} / {}", hbaseZkQuorum, hbaseTable);

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置Checkpoint（生产环境必须开启）
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 创建Kafka Source
        KafkaSource<VipEvent> kafkaSource = KafkaSource.<VipEvent>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new VipEventDeserializer())
                .build();

        // 读取Kafka数据流
        DataStream<VipEvent> vipEventStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-VIP-Events")
                .name("kafka-source")
                .uid("kafka-source");

        // 按order_no分区处理，确保同一订单的事件顺序处理
        DataStream<VipEvent> processedStream = vipEventStream
                .filter(event -> event != null && event.getOrderNo() != null)
                .name("filter-valid-events")
                .uid("filter-valid-events")
                .keyBy(VipEvent::getOrderNo)
                .process(new VipOrderProcessFunction(redisHost, redisPort, redisPassword, orderTtlSeconds))
                .name("vip-order-process")
                .uid("vip-order-process");

        // 写入HBase（仅vip_pay_success事件会到达这里）
        processedStream
                .addSink(new HBaseVipSinkFunction(hbaseZkQuorum, hbaseTable))
                .name("hbase-sink")
                .uid("hbase-sink");

        // 执行作业
        env.execute("VIP Order Process Job");
    }

    /**
     * VipEvent JSON反序列化器
     */
    public static class VipEventDeserializer extends AbstractDeserializationSchema<VipEvent> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper objectMapper;

        @Override
        public void open(InitializationContext context) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public VipEvent deserialize(byte[] message) throws IOException {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            try {
                return objectMapper.readValue(message, VipEvent.class);
            } catch (Exception e) {
                LOG.warn("Failed to deserialize message: {}", new String(message), e);
                return null;
            }
        }
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}

package com.bili.flink.vip;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * VIP订单处理函数
 * - vip_create_order: 缓存订单到Redis
 * - vip_pay_success: 删除Redis缓存，输出到下游写HBase
 * - vip_pay_fail/vip_pay_cancel: 删除Redis缓存
 */
public class VipOrderProcessFunction
        extends KeyedProcessFunction<String, VipEvent, VipEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(VipOrderProcessFunction.class);
    private static final long serialVersionUID = 1L;

    // Redis配置
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;
    private final int orderTtlSeconds;
    private final String redisKeyPrefix;

    // Redis连接池（transient避免序列化）
    private transient JedisPool jedisPool;
    private transient ObjectMapper objectMapper;

    public VipOrderProcessFunction(String redisHost, int redisPort,
                                   String redisPassword, int orderTtlSeconds) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.orderTtlSeconds = orderTtlSeconds;
        this.redisKeyPrefix = "vip:order:";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化Redis连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);

        if (redisPassword != null && !redisPassword.isEmpty()) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPassword);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000);
        }

        objectMapper = new ObjectMapper();

        LOG.info("VipOrderProcessFunction initialized, Redis: {}:{}", redisHost, redisPort);
    }

    @Override
    public void processElement(VipEvent event,
                               KeyedProcessFunction<String, VipEvent, VipEvent>.Context ctx,
                               Collector<VipEvent> out) throws Exception {
        String eventId = event.getEventId();
        String orderNo = event.getOrderNo();

        if (orderNo == null || orderNo.isEmpty()) {
            LOG.debug("Skipping event without order_no: {}", eventId);
            return;
        }

        String redisKey = redisKeyPrefix + orderNo;

        try (Jedis jedis = jedisPool.getResource()) {
            switch (eventId) {
                case "vip_create_order":
                    handleCreateOrder(jedis, redisKey, event);
                    break;

                case "vip_pay_success":
                    handlePaySuccess(jedis, redisKey, event, out);
                    break;

                case "vip_pay_fail":
                case "vip_pay_cancel":
                    handlePayFailOrCancel(jedis, redisKey, event);
                    break;

                default:
                    LOG.debug("Ignoring event type: {}", eventId);
            }
        } catch (Exception e) {
            LOG.error("Error processing event: {}, orderNo: {}", eventId, orderNo, e);
            throw e;
        }
    }

    /**
     * 处理创建订单事件 - 缓存到Redis
     */
    private void handleCreateOrder(Jedis jedis, String redisKey, VipEvent event) {
        try {
            String orderData = objectMapper.writeValueAsString(event);
            jedis.setex(redisKey, orderTtlSeconds, orderData);
            LOG.info("Order cached: orderNo={}, ttl={}s", event.getOrderNo(), orderTtlSeconds);
        } catch (Exception e) {
            LOG.error("Failed to cache order: {}", event.getOrderNo(), e);
        }
    }

    /**
     * 处理支付成功事件 - 删除Redis缓存，输出到下游写HBase
     */
    private void handlePaySuccess(Jedis jedis, String redisKey,
                                  VipEvent event, Collector<VipEvent> out) {
        String orderData = jedis.get(redisKey);
        if (orderData == null) {
            LOG.warn("Order not found in Redis (expired or already processed): orderNo={}",
                    event.getOrderNo());
            out.collect(event);
        } else {
            jedis.del(redisKey);
            LOG.info("Order completed: orderNo={}", event.getOrderNo());
            out.collect(event);
        }
    }

    /**
     * 处理支付失败/取消事件 - 删除Redis缓存
     */
    private void handlePayFailOrCancel(Jedis jedis, String redisKey, VipEvent event) {
        Long deleted = jedis.del(redisKey);
        LOG.info("Order {} ({}): orderNo={}, redis_deleted={}",
                event.getEventId().equals("vip_pay_fail") ? "failed" : "cancelled",
                event.getEventId(), event.getOrderNo(), deleted);
    }

    @Override
    public void close() throws Exception {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            LOG.info("Redis connection pool closed");
        }
    }
}

package com.bili.flink.vip;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * VIP事件POJO - 对应Kafka app_event_vip主题的消息结构
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VipEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("trace_id")
    private String traceId;

    @JsonProperty("session_id")
    private String sessionId;

    private Long mid;

    @JsonProperty("order_no")
    private String orderNo;

    @JsonProperty("client_ts")
    private Long clientTs;

    @JsonProperty("server_ts")
    private Long serverTs;

    @JsonProperty("url_path")
    private String urlPath;

    private String referer;
    private String ua;
    private String ip;

    private VipProperties properties;

    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public Long getMid() { return mid; }
    public void setMid(Long mid) { this.mid = mid; }

    public String getOrderNo() { return orderNo; }
    public void setOrderNo(String orderNo) { this.orderNo = orderNo; }

    public Long getClientTs() { return clientTs; }
    public void setClientTs(Long clientTs) { this.clientTs = clientTs; }

    public Long getServerTs() { return serverTs; }
    public void setServerTs(Long serverTs) { this.serverTs = serverTs; }

    public String getUrlPath() { return urlPath; }
    public void setUrlPath(String urlPath) { this.urlPath = urlPath; }

    public String getReferer() { return referer; }
    public void setReferer(String referer) { this.referer = referer; }

    public String getUa() { return ua; }
    public void setUa(String ua) { this.ua = ua; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public VipProperties getProperties() { return properties; }
    public void setProperties(VipProperties properties) { this.properties = properties; }

    @Override
    public String toString() {
        return "VipEvent{" +
                "eventId='" + eventId + "'" +
                ", mid=" + mid +
                ", orderNo='" + orderNo + "'" +
                ", serverTs=" + serverTs + "}";
    }

    /**
     * VIP事件属性
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class VipProperties implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("event_type")
        private String eventType;

        @JsonProperty("current_vip_type")
        private Integer currentVipType;

        @JsonProperty("current_vip_expire")
        private Long currentVipExpire;

        @JsonProperty("order_no")
        private String orderNo;

        @JsonProperty("plan_id")
        private String planId;

        @JsonProperty("plan_name")
        private String planName;

        @JsonProperty("original_price")
        private Integer originalPrice;

        @JsonProperty("final_price")
        private Integer finalPrice;

        @JsonProperty("pay_method")
        private String payMethod;

        private Integer amount;

        @JsonProperty("new_vip_expire")
        private Long newVipExpire;

        @JsonProperty("error_code")
        private String errorCode;

        @JsonProperty("error_msg")
        private String errorMsg;

        // Getters and Setters
        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }

        public Integer getCurrentVipType() { return currentVipType; }
        public void setCurrentVipType(Integer currentVipType) { this.currentVipType = currentVipType; }

        public Long getCurrentVipExpire() { return currentVipExpire; }
        public void setCurrentVipExpire(Long currentVipExpire) { this.currentVipExpire = currentVipExpire; }

        public String getOrderNo() { return orderNo; }
        public void setOrderNo(String orderNo) { this.orderNo = orderNo; }

        public String getPlanId() { return planId; }
        public void setPlanId(String planId) { this.planId = planId; }

        public String getPlanName() { return planName; }
        public void setPlanName(String planName) { this.planName = planName; }

        public Integer getOriginalPrice() { return originalPrice; }
        public void setOriginalPrice(Integer originalPrice) { this.originalPrice = originalPrice; }

        public Integer getFinalPrice() { return finalPrice; }
        public void setFinalPrice(Integer finalPrice) { this.finalPrice = finalPrice; }

        public String getPayMethod() { return payMethod; }
        public void setPayMethod(String payMethod) { this.payMethod = payMethod; }

        public Integer getAmount() { return amount; }
        public void setAmount(Integer amount) { this.amount = amount; }

        public Long getNewVipExpire() { return newVipExpire; }
        public void setNewVipExpire(Long newVipExpire) { this.newVipExpire = newVipExpire; }

        public String getErrorCode() { return errorCode; }
        public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

        public String getErrorMsg() { return errorMsg; }
        public void setErrorMsg(String errorMsg) { this.errorMsg = errorMsg; }
    }
}

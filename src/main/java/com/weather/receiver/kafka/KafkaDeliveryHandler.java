package com.weather.receiver.kafka;

import com.weather.info.avro.WeatherData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import io.micrometer.core.instrument.Metrics;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.messaging.support.MessageBuilder;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaDeliveryHandler {
    @Value("${tenantId:}")
    protected String tenantId = "";
    @Autowired
    KafkaTemplate<String, GenericRecord> template;
    private static String TIMESTAMP = "timestamp";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDeliveryHandler.class);
    public void deliverAvroRecord(GenericRecord payload) throws Exception {
        if (validatePayloadTS(payload)) {
            deliver(payload);
        } else {
            LOGGER.debug("Not delivering invalid payload: {}", payload);
        }
    }
    private boolean validatePayloadTS(GenericRecord payload) {
        long payloadTS = 0;
        if (payload.getSchema().getField(TIMESTAMP) != null) {
            Object timestampObj = payload.get(TIMESTAMP);
            if (timestampObj != null) {
                payloadTS = ((Long) timestampObj);
            }
        }
        long limit = Instant.now().toEpochMilli() + (60_000);
        if (payloadTS > limit) {
            LOGGER.warn(
                    "Packet is from far future:\nCurrent:{},\nTimestamp:{}\nPayload:{}",
                    limit, payloadTS, payload);
            return false;
        }
        if (payloadTS <= 0) {
            LOGGER.warn("Invalid TS in payload: {}", payload);
            return false;
        }
        return true;
    }

    public void deliver(GenericRecord payload) throws Exception {
        sendToKafka(payload, getTopicFromPayload(payload));
    }

    private void sendToKafka(GenericRecord payload, String topic) throws Exception {
        long payloadTimestamp = 0;
        try {
            long payloadTS = 0;
            if (payload.getSchema().getField(TIMESTAMP) != null) {
                Object objTS = payload.get(TIMESTAMP);
                if (objTS != null) {
                    payloadTS = ((Long) objTS);
                }
            }
            StringBuilder tenantTopic = new StringBuilder(topic);
            if (StringUtils.isNotBlank(tenantId)) {
                tenantTopic.append("-").append(tenantId);
            }
            MessageBuilder<GenericRecord> msgBuilder =
                    MessageBuilder.withPayload(payload)
                            .setHeader(KafkaHeaders.TOPIC, tenantTopic.toString())
                            .setHeader(KafkaHeaders.TIMESTAMP, payloadTS);
            addKafkaParitionKey(msgBuilder, payload);
            CompletableFuture<SendResult<String, GenericRecord>> future =
                    template.send(msgBuilder.build());
            future.whenComplete(
                    (res, ex) -> {
                        if (ex != null) {
                            LOGGER.error("Error posting data to kafka", ex);
                        } else {
                            Metrics.counter(
                                            String.join("-", payload.getSchema().getName(), "sent"),
                                            "topic",
                                            topic,
                                            "vendor",
                                            getVendorFromPayload(payload))
                                    .increment();
                        }
                    });
        } catch (Exception e) {
            throw new Exception("Can't send message", e);
        }
    }
    private void addKafkaParitionKey(
            MessageBuilder<GenericRecord> msgPayloadBuilder, GenericRecord payload) {
        switch (payload.getSchema().getName()) {
            case "WeatherData":
                msgPayloadBuilder.setHeader(
                        KafkaHeaders.KEY, ((WeatherData) payload).getDeviceId().toString());
                break;
            default:
                break;
        }
    }
    public static String getTopicFromPayload(GenericRecord payload) {
        switch (payload.getSchema().getName()) {
            case "WeatherData":
                return "weatherData";
            default:
                throw new RuntimeException("Topic not know for payload: "
                        + payload.getSchema().getFullName());
        }
    }
    private String getVendorFromPayload(GenericRecord payload) {
        if (payload.getSchema().getField("vendor") != null) {
            return payload.get("vendor").toString();
        } else {
            return "PAYLOAD-WITH-NO-VENDOR";
        }
    }
}

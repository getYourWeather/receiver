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
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDeliveryHandler.class);
    public void deliverAvroRecord(GenericRecord payload) throws Exception {
        boolean validPayload = validatePayloadTS(payload);
        if (validPayload) {
            deliver(payload);
        } else {
            LOGGER.debug("Not delivering invalid payload: {}", payload);
        }
    }
    private boolean validatePayloadTS(GenericRecord payload) {
        long payloadTimestamp = 0;
        long currentTimestamp = Instant.now().toEpochMilli();
        if (payload.getSchema().getField("timestamp") != null) {
            Object timestampObj = payload.get("timestamp");
            if (timestampObj != null) {
                payloadTimestamp = ((Long) timestampObj);
            }
        }
        long limit = currentTimestamp + (5 * 60 * 1000);
        if (payloadTimestamp > limit) {
            LOGGER.warn(
                    "Ignoring packet since it's too far in future:\nCurrent:{},\nTimestamp:{}\nPayload:{}",
                    limit,
                    payloadTimestamp,
                    payload);
            return false;
        }
        if (payloadTimestamp <= 0) {
            LOGGER.warn("Payload timestamp is 0 for payload {}", payload);
            return false;
        }
        return true;
    }

    public void deliver(GenericRecord payload) throws Exception {
        deliverAvroRecordViaKafka(payload, getTopicForPayload(payload));
    }

    private void deliverAvroRecordViaKafka(GenericRecord payload, String topic) throws Exception {
        long payloadTimestamp = 0;
        try {
            if (payload.getSchema().getField("timestamp") != null) {
                Object timestampObj = payload.get("timestamp");
                if (timestampObj != null) {
                    payloadTimestamp = ((Long) timestampObj);
                }
            }

            StringBuilder tenantTopic = new StringBuilder(topic);
            if (StringUtils.isNotBlank(tenantId)) {
                tenantTopic.append("-").append(tenantId);
            }

            MessageBuilder<GenericRecord> msgPayloadBuilder =
                    MessageBuilder.withPayload(payload)
                            .setHeader(KafkaHeaders.TOPIC, tenantTopic.toString())
                            .setHeader(KafkaHeaders.TIMESTAMP, payloadTimestamp);
            addKafkaParitionKey(msgPayloadBuilder, payload);
            CompletableFuture<SendResult<String, GenericRecord>> future =
                    template.send(msgPayloadBuilder.build());
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
                                            getVendor(payload))
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
    public static String getTopicForPayload(GenericRecord payload) {
        switch (payload.getSchema().getName()) {
            case "WeatherData":
                return "weatherData";
            default:
                throw new RuntimeException(
                        "Kafka topic not known for the message type of class: "
                                + payload.getSchema().getFullName());
        }
    }
    private String getVendor(GenericRecord payload) {
        if (payload.getSchema().getField("vendor") != null) {
            return payload.get("vendor").toString();
        } else {
            return "not-set";
        }
    }
}

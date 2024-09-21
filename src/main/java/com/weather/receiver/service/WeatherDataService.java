package com.weather.receiver.service;

import com.weather.info.avro.WeatherData;
import com.weather.receiver.dto.WeatherDataDTO;
import com.weather.receiver.kafka.KafkaDeliveryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WeatherDataService {

    @Autowired
    KafkaDeliveryHandler kafkaDeliveryHandler;

    public void processWeatherData (WeatherDataDTO weatherDataDTO) throws Exception {
        WeatherData weatherData = weatherDataDTO.toAvroRecord();
        this.kafkaDeliveryHandler.deliverAvroRecord(weatherData);
    }
}

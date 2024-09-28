package com.weather.receiver.controller;

import com.weather.receiver.dto.WeatherDataDTO;
import com.weather.receiver.service.WeatherDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/weather/data")
public class WeatherController {
    @Autowired
    WeatherDataService weatherDataService;
    @PostMapping("/push")
    public void getWeatherData(@RequestBody WeatherDataDTO weatherDataDTO) throws Exception {
        this.weatherDataService.processWeatherData(weatherDataDTO);
    }
}

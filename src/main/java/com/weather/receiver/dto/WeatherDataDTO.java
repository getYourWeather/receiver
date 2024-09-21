package com.weather.receiver.dto;

import com.weather.info.avro.*;

public class WeatherDataDTO {
    private String devId;
    private long timestamp;
    private LocationDTO location;
    private TemperatureDTO temperature;
    private HumidityDTO humidity;
    private PressureDTO pressure;
    private WindDTO wind;

    public String getDevId() {
        return devId;
    }
    public void setDevId(String devId) {
        this.devId = devId;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public LocationDTO getLocation() {
        return location;
    }
    public void setLocation(LocationDTO location) {
        this.location = location;
    }
    public TemperatureDTO getTemperature() {
        return temperature;
    }
    public void setTemperature(TemperatureDTO temperature) {
        this.temperature = temperature;
    }
    public HumidityDTO getHumidity() {
        return humidity;
    }
    public void setHumidity(HumidityDTO humidity) {
        this.humidity = humidity;
    }
    public PressureDTO getPressure() {
        return pressure;
    }
    public void setPressure(PressureDTO pressure) {
        this.pressure = pressure;
    }
    public WindDTO getWind() {
        return wind;
    }
    public void setWind(WindDTO wind) {
        this.wind = wind;
    }

    public WeatherData toAvroRecord () {
        return WeatherData.newBuilder()
                .setDeviceId(this.devId)
                .setTimestamp(this.getTimestamp())
                .setHumidity(this.humidity.toAvroRecord())
                .setLocation(this.location.toAvroRecord())
                .setPressure(this.pressure.toAvroRecord())
                .setTemperature(this.temperature.toAvroRecord())
                .setWind(this.wind.toAvroRecord())
                .build();
    }

    // Nested DTO Classes
    public static class LocationDTO {
        private double latitude;
        private double longitude;

        public double getLatitude() {
            return latitude;
        }
        public void setLatitude(double latitude) {
            this.latitude = latitude;
        }
        public double getLongitude() {
            return longitude;
        }
        public void setLongitude(double longitude) {
            this.longitude = longitude;
        }

        public Location toAvroRecord () {
            return Location.newBuilder()
                    .setLatitude(this.latitude)
                    .setLongitude(this.longitude)
                    .build();
        }
    }

    public static class TemperatureDTO {
        private double current;
        private String unit;
        public double getCurrent() {
            return current;
        }
        public void setCurrent(double current) {
            this.current = current;
        }
        public String getUnit() {
            return unit;
        }
        public void setUnit(String unit) {
            this.unit = unit;
        }
        public Temperature toAvroRecord() {
            return Temperature.newBuilder()
                    .setCurrent(this.current)
                    .setUnit(this.unit)
                    .build();
        }
    }

    public static class HumidityDTO {
        private int value;
        private String unit;

        // Getters and Setters

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public String getUnit() {
            return unit;
        }

        public void setUnit(String unit) {
            this.unit = unit;
        }
        private Humidity toAvroRecord () {
            return Humidity.newBuilder()
                    .setValue(this.value)
                    .setUnit(this.unit)
                    .build();
        }
    }

    public static class PressureDTO {
        private int value;
        private String unit;

        // Getters and Setters

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public String getUnit() {
            return unit;
        }

        public void setUnit(String unit) {
            this.unit = unit;
        }

        public Pressure toAvroRecord() {
            return Pressure.newBuilder()
                    .setValue(this.value)
                    .setUnit(this.unit)
                    .build();
        }
    }

    public static class WindDTO {
        public enum Unit {
            KILOMETERS_PER_HOUR,
            METERS_PER_SECOND,
            MILES_PER_HOUR
        }
        private double speed;
        private Unit unit;
        private String direction;
        private int degree;
        private double gustSpeed;

        public double getSpeed() {
            return speed;
        }

        public void setSpeed(double speed) {
            this.speed = speed;
        }

        public Unit getUnit() {
            return unit;
        }

        public void setUnit(Unit unit) {
            this.unit = unit;
        }

        public String getDirection() {
            return direction;
        }

        public void setDirection(String direction) {
            this.direction = direction;
        }

        public int getDegree() {
            return degree;
        }

        public void setDegree(int degree) {
            this.degree = degree;
        }

        public double getGustSpeed() {
            return gustSpeed;
        }

        public void setGustSpeed(double gustSpeed) {
            this.gustSpeed = gustSpeed;
        }

        public Wind toAvroRecord() {
            return Wind.newBuilder()
                    .setSpeed(this.speed)
                    .setDirection(this.direction)
                    .setUnit(this.unit.toString())
                    .setGustSpeed(this.gustSpeed)
                    .setDegree(this.degree)
                    .build();
        }
    }
}
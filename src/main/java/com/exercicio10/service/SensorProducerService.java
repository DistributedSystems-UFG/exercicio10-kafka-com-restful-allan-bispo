package com.exercicio10.service;

import com.exercicio10.config.KafkaTopicsConfig;
import com.exercicio10.model.TemperatureRaw;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Service
public class SensorProducerService {

    private static final Logger log = LoggerFactory.getLogger(SensorProducerService.class);
    private static final String[] SENSORS = {"sensor-01", "sensor-02", "sensor-03"};
    private static final double TEMP_MIN = 10.0;
    private static final double TEMP_MAX = 45.0;
    private static final double VARIATION_STEP = 0.4;
    private static final double EMIT_THRESHOLD = 1.0;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    private final Map<String, Double> currentTemp = new HashMap<>();
    private final Map<String, Double> lastEmittedTemp = new HashMap<>();

    public SensorProducerService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;

        for (String sensor : SENSORS) {
            double initial = 20.0 + random.nextDouble() * 10;
            currentTemp.put(sensor, initial);
            lastEmittedTemp.put(sensor, null);
        }
    }

    @Scheduled(fixedDelay = 500)
    public void simulateSensors() {
        for (String sensorId : SENSORS) {
            double current = currentTemp.get(sensorId);
            double delta = (random.nextDouble() * 2 - 1) * VARIATION_STEP;
            double next = Math.max(TEMP_MIN, Math.min(TEMP_MAX, current + delta));
            currentTemp.put(sensorId, next);

            Double lastEmitted = lastEmittedTemp.get(sensorId);
            if (lastEmitted == null || Math.abs(next - lastEmitted) >= EMIT_THRESHOLD) {
                emit(sensorId, next);
                lastEmittedTemp.put(sensorId, next);
            }
        }
    }

    private void emit(String sensorId, double value) {
        try {
            TemperatureRaw raw = new TemperatureRaw(sensorId, value, System.currentTimeMillis() / 1000);
            String json = objectMapper.writeValueAsString(raw);
            kafkaTemplate.send(KafkaTopicsConfig.TOPIC_RAW, sensorId, json);
            log.info("[PRODUCER] {} -> {}", sensorId, String.format("%.2f°C", value));
        } catch (JsonProcessingException e) {
            log.error("Erro ao serializar leitura do sensor {}", sensorId, e);
        }
    }
}

package com.exercicio10.service;

import com.exercicio10.config.KafkaTopicsConfig;
import com.exercicio10.model.TemperatureRaw;
import com.exercicio10.model.TemperatureStats;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

@Service
public class TemperatureProcessorService {

    private static final Logger log = LoggerFactory.getLogger(TemperatureProcessorService.class);
    private static final long WINDOW_SECONDS = 7200; // 2 horas

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Janela deslizante por sensor: [timestamp_epoch_s, value]
    private final Map<String, Deque<double[]>> windows = new HashMap<>();

    public TemperatureProcessorService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopicsConfig.TOPIC_RAW, groupId = "processor-group")
    public void process(String message) {
        try {
            TemperatureRaw raw = objectMapper.readValue(message, TemperatureRaw.class);
            String sensorId = raw.getSensorId();
            long now = raw.getTimestamp();

            windows.computeIfAbsent(sensorId, k -> new ArrayDeque<>());
            Deque<double[]> window = windows.get(sensorId);

            // Remove entradas fora da janela de 2 horas
            while (!window.isEmpty() && (now - window.peekFirst()[0]) > WINDOW_SECONDS) {
                window.pollFirst();
            }

            window.addLast(new double[]{now, raw.getValue()});

            // Calcula estatísticas da janela atual
            double sum = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
            for (double[] entry : window) {
                sum += entry[1];
                if (entry[1] < min) min = entry[1];
                if (entry[1] > max) max = entry[1];
            }
            double avg = sum / window.size();

            TemperatureStats stats = new TemperatureStats(sensorId, avg, min, max, window.size(), now);
            String json = objectMapper.writeValueAsString(stats);
            kafkaTemplate.send(KafkaTopicsConfig.TOPIC_PROCESSED, sensorId, json);

            log.info("[PROCESSOR] {} | avg={} min={} max={} n={} (janela 2h)",
                    sensorId,
                    String.format("%.2f", avg),
                    String.format("%.2f", min),
                    String.format("%.2f", max),
                    window.size());

        } catch (JsonProcessingException e) {
            log.error("Erro ao processar mensagem: {}", message, e);
        }
    }
}

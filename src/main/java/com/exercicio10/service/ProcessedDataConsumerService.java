package com.exercicio10.service;

import com.exercicio10.config.KafkaTopicsConfig;
import com.exercicio10.entity.TemperatureStatsEntity;
import com.exercicio10.model.TemperatureStats;
import com.exercicio10.repository.TemperatureStatsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProcessedDataConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ProcessedDataConsumerService.class);

    private final TemperatureStatsRepository repository;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    public ProcessedDataConsumerService(TemperatureStatsRepository repository,
                                        ObjectMapper objectMapper,
                                        ApplicationEventPublisher eventPublisher) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
    }

    @KafkaListener(topics = KafkaTopicsConfig.TOPIC_PROCESSED, groupId = "server-group")
    public void consume(String message) {
        try {
            TemperatureStats stats = objectMapper.readValue(message, TemperatureStats.class);
            TemperatureStatsEntity entity = new TemperatureStatsEntity(
                    stats.getSensorId(),
                    stats.getAvg2h(),
                    stats.getMin2h(),
                    stats.getMax2h(),
                    stats.getCount2h(),
                    stats.getTimestamp()
            );
            repository.save(entity);
            eventPublisher.publishEvent(new StatsUpdatedEvent(this, entity));
            log.info("[SERVER] Persistido: {} @ {}", stats.getSensorId(), stats.getTimestamp());
        } catch (JsonProcessingException e) {
            log.error("Erro ao consumir mensagem processada: {}", message, e);
        }
    }
}

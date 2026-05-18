package com.exercicio10.service;

import com.exercicio10.entity.TemperatureStatsEntity;
import org.springframework.context.ApplicationEvent;

public class StatsUpdatedEvent extends ApplicationEvent {

    private final TemperatureStatsEntity stats;

    public StatsUpdatedEvent(Object source, TemperatureStatsEntity stats) {
        super(source);
        this.stats = stats;
    }

    public TemperatureStatsEntity getStats() {
        return stats;
    }
}

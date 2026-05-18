package com.exercicio10.repository;

import com.exercicio10.entity.TemperatureStatsEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface TemperatureStatsRepository extends JpaRepository<TemperatureStatsEntity, Long> {

    @Query("SELECT DISTINCT e.sensorId FROM TemperatureStatsEntity e ORDER BY e.sensorId")
    List<String> findDistinctSensorIds();

    Optional<TemperatureStatsEntity> findTopBySensorIdOrderByTimestampDesc(String sensorId);

    List<TemperatureStatsEntity> findBySensorIdOrderByTimestampDesc(String sensorId);

    @Query("SELECT e FROM TemperatureStatsEntity e WHERE e.sensorId = :sensorId ORDER BY e.timestamp DESC LIMIT :limit")
    List<TemperatureStatsEntity> findHistoryBySensorId(@Param("sensorId") String sensorId, @Param("limit") int limit);
}

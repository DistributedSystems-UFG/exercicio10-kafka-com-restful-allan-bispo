package com.exercicio10.controller;

import com.exercicio10.entity.TemperatureStatsEntity;
import com.exercicio10.repository.TemperatureStatsRepository;
import com.exercicio10.service.StatsUpdatedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/api/sensors")
public class TemperatureController {

    private final TemperatureStatsRepository repository;

    // Emissores SSE ativos por sensor (equivalente ao streaming do gRPC)
    private final Map<String, List<SseEmitter>> emitters = new ConcurrentHashMap<>();

    public TemperatureController(TemperatureStatsRepository repository) {
        this.repository = repository;
    }

    /**
     * Lista todos os sensores conhecidos.
     * Equivale ao RPC ListSensors(Empty) returns (SensorList) do gRPC.
     */
    @GetMapping
    public ResponseEntity<List<String>> listSensors() {
        List<String> sensors = repository.findDistinctSensorIds();
        return ResponseEntity.ok(sensors);
    }

    /**
     * Retorna as estatísticas mais recentes de um sensor.
     * Equivale ao RPC GetLatestStats(SensorRequest) returns (TemperatureStats) do gRPC.
     */
    @GetMapping("/{sensorId}/latest")
    public ResponseEntity<TemperatureStatsEntity> getLatestStats(@PathVariable String sensorId) {
        Optional<TemperatureStatsEntity> latest = repository.findTopBySensorIdOrderByTimestampDesc(sensorId);
        return latest
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Retorna o histórico de estatísticas de um sensor (paginado por limit).
     * Equivale ao RPC GetHistory(HistoryRequest) returns (stream TemperatureStats) do gRPC,
     * porém entregando os dados de uma vez como lista JSON em vez de streaming bidirecional.
     */
    @GetMapping("/{sensorId}/history")
    public ResponseEntity<List<TemperatureStatsEntity>> getHistory(
            @PathVariable String sensorId,
            @RequestParam(defaultValue = "10") int limit) {
        List<TemperatureStatsEntity> history = repository.findHistoryBySensorId(sensorId, limit);
        if (history.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(history);
    }

    /**
     * Streaming em tempo real via Server-Sent Events (SSE).
     * Substitui o server-streaming nativo do gRPC com uma abordagem HTTP padrão.
     * O cliente mantém a conexão aberta e recebe eventos conforme novos dados chegam.
     */
    @GetMapping("/{sensorId}/stream")
    public SseEmitter streamStats(@PathVariable String sensorId) {
        SseEmitter emitter = new SseEmitter(0L); // sem timeout
        emitters.computeIfAbsent(sensorId, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(sensorId, emitter));
        emitter.onTimeout(() -> removeEmitter(sensorId, emitter));
        emitter.onError(e -> removeEmitter(sensorId, emitter));

        // Envia o dado mais recente imediatamente ao conectar
        repository.findTopBySensorIdOrderByTimestampDesc(sensorId).ifPresent(latest -> {
            try {
                emitter.send(SseEmitter.event().name("stats").data(latest));
            } catch (IOException ignored) {}
        });

        return emitter;
    }

    /**
     * Recebe evento de novo dado processado e notifica clientes SSE conectados.
     */
    @EventListener
    public void onStatsUpdated(StatsUpdatedEvent event) {
        TemperatureStatsEntity stats = event.getStats();
        List<SseEmitter> sensorEmitters = emitters.get(stats.getSensorId());
        if (sensorEmitters == null) return;
        sensorEmitters.removeIf(emitter -> {
            try {
                emitter.send(SseEmitter.event().name("stats").data(stats));
                return false;
            } catch (IOException e) {
                return true;
            }
        });
    }

    private void removeEmitter(String sensorId, SseEmitter emitter) {
        List<SseEmitter> list = emitters.get(sensorId);
        if (list != null) list.remove(emitter);
    }
}

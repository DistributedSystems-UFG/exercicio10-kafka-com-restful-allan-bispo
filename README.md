[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/wWsgWD6e)

# Exercício 10 — Kafka com RESTful (Java)

Sistema de monitoramento de temperatura de sensores IoT usando Apache Kafka para processamento de eventos e RESTful para consultas. É a reescrita em Java do Exercício 09, substituindo o gRPC por REST/SSE.

---

## Arquitetura

```
SensorProducerService (@Scheduled)
        |
        | temperature-raw (Kafka)
        v
TemperatureProcessorService (@KafkaListener)
  - Janela deslizante de 2h por sensor
  - Calcula avg, min, max, count
        |
        | temperature-processed (Kafka)
        v
ProcessedDataConsumerService (@KafkaListener)
  - Persiste no banco H2
  - Publica ApplicationEvent
        |
        v
TemperatureController (REST + SSE)
  - GET /api/sensors
  - GET /api/sensors/{id}/latest
  - GET /api/sensors/{id}/history
  - GET /api/sensors/{id}/stream  <- SSE
```

---

## Endpoints REST

| Método | Endpoint                          | Descricao                                               | Equivalente gRPC              |
|--------|-----------------------------------|---------------------------------------------------------|-------------------------------|
| GET    | `/api/sensors`                    | Lista todos os sensores                                  | `ListSensors(Empty)`          |
| GET    | `/api/sensors/{id}/latest`        | Estatísticas mais recentes do sensor                    | `GetLatestStats(SensorRequest)` |
| GET    | `/api/sensors/{id}/history?limit` | Histórico de N registros                                | `GetHistory(HistoryRequest)`  |
| GET    | `/api/sensors/{id}/stream`        | Streaming em tempo real via SSE                         | `stream TemperatureStats`     |

---

## Como executar

### 1. Subir a infraestrutura Kafka

```bash
docker-compose up -d
```

### 2. Compilar e iniciar a aplicação

```bash
mvn spring-boot:run
```

A aplicação inicia na porta `8080`. O console H2 fica disponível em `http://localhost:8080/h2-console` (JDBC URL: `jdbc:h2:file:./temperatura`).

### 3. Exemplos de uso com curl

```bash
# Listar sensores disponíveis
curl http://localhost:8080/api/sensors

# Estatísticas mais recentes do sensor-01
curl http://localhost:8080/api/sensors/sensor-01/latest

# Últimos 5 registros do sensor-02
curl http://localhost:8080/api/sensors/sensor-02/history?limit=5

# Streaming em tempo real (Server-Sent Events)
curl -N http://localhost:8080/api/sensors/sensor-01/stream
```

---

## Stack tecnológica

| Componente       | Exercício 09 (gRPC)        | Exercício 10 (REST)         |
|------------------|----------------------------|-----------------------------|
| Linguagem        | Python 3.9+                | Java 17                     |
| Framework        | —                          | Spring Boot 3.2             |
| Comunicação      | gRPC / Protocol Buffers    | HTTP/JSON (REST + SSE)      |
| Banco de dados   | SQLite                     | H2 (file-based)             |
| Kafka Client     | kafka-python               | Spring Kafka                |
| Build            | pip / requirements.txt     | Maven / pom.xml             |

---

## Comparação: gRPC vs RESTful

### Critérios de Desenvolvimento

| Critério                    | gRPC                                                                                 | RESTful                                                                               |
|-----------------------------|--------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| **Contrato de API**         | Fortemente tipado via `.proto`; contrato é a única fonte de verdade                  | Informal por padrão (JSON livre); requer documentação extra (OpenAPI/Swagger)         |
| **Geração de código**       | Stubs gerados automaticamente para múltiplas linguagens (`protoc`)                   | Código escrito manualmente; clientes precisam saber o schema                          |
| **Curva de aprendizado**    | Alta: requer conhecimento de Protobuf, gRPC, compilação de stubs                     | Baixa: HTTP/JSON são universalmente conhecidos                                        |
| **Streaming**               | Nativo: unário, server-streaming, client-streaming, bidirecional                     | Requer SSE ou WebSocket; SSE é server-only, WebSocket é mais complexo                 |
| **Serialização**            | Protobuf binário: eficiente, sem overhead de texto                                   | JSON texto: verboso, mas legível e depurável com ferramentas padrão (curl, browser)   |
| **Ferramentas de teste**    | Requer grpcurl, Postman (gRPC), ou clientes dedicados                                | curl, Postman, browser — qualquer cliente HTTP                                        |
| **Interoperabilidade**      | Excelente entre linguagens suportadas; difícil em browsers                           | Universal: qualquer linguagem ou plataforma com HTTP                                  |
| **Documentação**            | `.proto` serve como documentação viva                                                | OpenAPI/Swagger precisa ser mantido separadamente                                     |

### Critérios de Operação

| Critério                    | gRPC                                                                                 | RESTful                                                                               |
|-----------------------------|--------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| **Performance**             | Superior: HTTP/2 multiplexado + Protobuf binário reduzem latência e banda            | Inferior em alta frequência: HTTP/1.1 + JSON geram mais overhead                     |
| **Monitoramento**           | Requer ferramentas específicas (Envoy, gRPC-gateway, Prometheus gRPC exporter)       | Suporte nativo em todo o ecossistema: load balancers, APMs, logs HTTP padrão          |
| **Depuração**               | Difícil: payload binário não é legível; necessita de ferramentas especiais           | Simples: logs HTTP são legíveis; payload JSON inspecionável no browser ou curl        |
| **Compatibilidade de rede** | Requer HTTP/2; proxies e firewalls antigos podem bloquear                            | HTTP/1.1 funciona em qualquer infraestrutura de rede                                 |
| **Escalabilidade**          | Excelente com multiplexação HTTP/2 e header compression                              | Boa, mas conexões SSE são persistentes e podem sobrecarregar servidores               |
| **Cache**                   | Não suporta caching HTTP padrão (chamadas POST-like)                                 | Suporte nativo a cache HTTP (ETags, Cache-Control) em endpoints GET                  |
| **Versionamento**           | Evolutivo: campos novos no proto não quebram clientes antigos (backward compatible)  | Requer estratégia explícita: path versioning (`/v1/`), headers, ou query params       |
| **Deploy em cloud**         | Requer configuração adicional para Load Balancers que entendam HTTP/2/gRPC           | Suportado nativamente por qualquer cloud LB (AWS ALB, GCP, Azure, nginx)             |

### Quando usar cada um

**Prefira gRPC quando:**
- Comunicação interna entre microsserviços com alta frequência de chamadas
- Streaming bidirecional é necessário (ex: telemetria em tempo real)
- Performance e eficiência de banda são críticas
- Equipe controlada e linguagens previamente definidas

**Prefira RESTful quando:**
- APIs públicas ou abertas a clientes externos
- Integração com browsers sem biblioteca especial
- Equipes precisam de simplicidade para onboarding
- Infraestrutura legada ou limitada a HTTP/1.1
- Cacheabilidade e depuração simples são prioridade

### Conclusão

No contexto deste exercício (monitoramento de temperatura), a substituição do gRPC por REST simplificou o desenvolvimento (eliminou a compilação de `.proto` e geração de stubs) e facilitou a depuração. O SSE oferece streaming suficiente para o caso de uso. Contudo, em cenários de alta frequência de leituras (ex: milhares de sensores por segundo), o gRPC seria mais eficiente devido ao Protobuf binário e ao multiplexamento HTTP/2.

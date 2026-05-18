package com.exercicio10.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "temperature_stats", indexes = {
    @Index(name = "idx_sensor_timestamp", columnList = "sensorId, timestamp DESC")
})
public class TemperatureStatsEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String sensorId;

    private double avg2h;
    private double min2h;
    private double max2h;
    private int count2h;

    @Column(nullable = false)
    private long timestamp;

    public TemperatureStatsEntity() {}

    public TemperatureStatsEntity(String sensorId, double avg2h, double min2h, double max2h, int count2h, long timestamp) {
        this.sensorId = sensorId;
        this.avg2h = avg2h;
        this.min2h = min2h;
        this.max2h = max2h;
        this.count2h = count2h;
        this.timestamp = timestamp;
    }

    public Long getId() { return id; }

    public String getSensorId() { return sensorId; }
    public void setSensorId(String sensorId) { this.sensorId = sensorId; }

    public double getAvg2h() { return avg2h; }
    public void setAvg2h(double avg2h) { this.avg2h = avg2h; }

    public double getMin2h() { return min2h; }
    public void setMin2h(double min2h) { this.min2h = min2h; }

    public double getMax2h() { return max2h; }
    public void setMax2h(double max2h) { this.max2h = max2h; }

    public int getCount2h() { return count2h; }
    public void setCount2h(int count2h) { this.count2h = count2h; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}

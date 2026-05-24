package com.vbforge.kafkanotificationdemo.model;

import java.time.LocalDateTime;

public class Notification {

    private String id;
    private String message;
    private String type;
    private LocalDateTime timestamp;
    private boolean processed;

    public Notification() {
        this.timestamp = LocalDateTime.now();
        this.processed = false;
    }

    public Notification(String id, String message, String type) {
        this();
        this.id = id;
        this.message = message;
        this.type = type;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    @Override
    public String toString() {
        return "Notification{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", processed=" + processed +
                '}';
    }

}

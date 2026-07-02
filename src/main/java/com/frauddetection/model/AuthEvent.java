package com.frauddetection.model;

import java.util.Objects;

public class AuthEvent {

    private String eventId;
    private String userId;
    private String eventType;
    private String deviceId;
    private String ipAddress;
    private double latitude;
    private double longitude;
    private long timestamp;

    public AuthEvent() {}

    public AuthEvent(
        String eventId,
        String userId,
        String eventType,
        String deviceId,
        String ipAddress,
        double latitude,
        double longitude,
        long timestamp
    ) {
        this.eventId = eventId;
        this.userId = userId;
        this.eventType = eventType;
        this.deviceId = deviceId;
        this.ipAddress = ipAddress;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return (
            "AuthEvent{" +
            "eventId='" + eventId + '\'' +
            ", userId='" + userId + '\'' +
            ", eventType='" + eventType + '\'' +
            ", deviceId='" + deviceId + '\'' +
            ", ipAddress='" + ipAddress + '\'' +
            ", latitude=" + latitude +
            ", longitude=" + longitude +
            ", timestamp=" + timestamp +
            '}'
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthEvent authEvent = (AuthEvent) o;
        return (
            Double.compare(authEvent.latitude, latitude) == 0 &&
            Double.compare(authEvent.longitude, longitude) == 0 &&
            timestamp == authEvent.timestamp &&
            Objects.equals(eventId, authEvent.eventId) &&
            Objects.equals(userId, authEvent.userId) &&
            Objects.equals(eventType, authEvent.eventType) &&
            Objects.equals(deviceId, authEvent.deviceId) &&
            Objects.equals(ipAddress, authEvent.ipAddress)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, userId, eventType, deviceId, ipAddress, latitude, longitude, timestamp);
    }
}

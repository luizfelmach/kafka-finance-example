package com.frauddetection.model;

import java.util.Objects;

public class AuthEvent {

    private String eventId;
    private String userId;
    private String eventType;
    private String deviceId;
    private String ipAddress;

    public AuthEvent() {}

    public AuthEvent(
        String eventId,
        String userId,
        String eventType,
        String deviceId,
        String ipAddress
    ) {
        this.eventId = eventId;
        this.userId = userId;
        this.eventType = eventType;
        this.deviceId = deviceId;
        this.ipAddress = ipAddress;
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

    @Override
    public String toString() {
        return (
            "AuthEvent{" +
            "eventId='" +
            eventId +
            '\'' +
            ", userId='" +
            userId +
            '\'' +
            ", eventType='" +
            eventType +
            '\'' +
            ", deviceId='" +
            deviceId +
            '\'' +
            ", ipAddress='" +
            ipAddress +
            '\'' +
            '}'
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthEvent authEvent = (AuthEvent) o;
        return (
            Objects.equals(eventId, authEvent.eventId) &&
            Objects.equals(userId, authEvent.userId) &&
            Objects.equals(eventType, authEvent.eventType) &&
            Objects.equals(deviceId, authEvent.deviceId) &&
            Objects.equals(ipAddress, authEvent.ipAddress)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, userId, eventType, deviceId, ipAddress);
    }
}

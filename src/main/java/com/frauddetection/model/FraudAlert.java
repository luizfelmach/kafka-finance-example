package com.frauddetection.model;

import java.util.Objects;

public class FraudAlert {
    private String alertId;
    private String accountId;
    private String severity;
    private String alertType;
    private String userId;
    private String description;
    private long timestamp;

    public FraudAlert() {}

    public FraudAlert(String alertId, String accountId, String severity, String alertType, String userId, String description, long timestamp) {
        this.alertId = alertId;
        this.accountId = accountId;
        this.severity = severity;
        this.alertType = alertType;
        this.userId = userId;
        this.description = description;
        this.timestamp = timestamp;
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "FraudAlert{" +
                "alertId='" + alertId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", severity='" + severity + '\'' +
                ", alertType='" + alertType + '\'' +
                ", userId='" + userId + '\'' +
                ", description='" + description + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FraudAlert that = (FraudAlert) o;
        return timestamp == that.timestamp &&
                Objects.equals(alertId, that.alertId) &&
                Objects.equals(accountId, that.accountId) &&
                Objects.equals(severity, that.severity) &&
                Objects.equals(alertType, that.alertType) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId, accountId, severity, alertType, userId, description, timestamp);
    }
}

package com.frauddetection.model;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

public class FraudAlert {
    private String alertId;
    private String accountId;
    private String severity;
    private String alertType;
    private String userId;
    private String description;
    private long timestamp;
    private Double latitude;
    private Double longitude;

    public FraudAlert() {}

    public FraudAlert(String alertId, String accountId, String severity, String alertType, String userId, String description, long timestamp) {
        this(alertId, accountId, severity, alertType, userId, description, timestamp, null, null);
    }

    public FraudAlert(String alertId, String accountId, String severity, String alertType, String userId, String description, long timestamp, Double latitude, Double longitude) {
        this.alertId = alertId;
        this.accountId = accountId;
        this.severity = severity;
        this.alertType = alertType;
        this.userId = userId;
        this.description = description;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
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

    public Double getLatitude() { return latitude; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }
    public Double getLongitude() { return longitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }

    public static FraudAlert highValue(TransactionEvent tx) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            tx.getAccountId(),
            "HIGH",
            "HIGH_VALUE_TRANSACTION",
            tx.getUserId(),
            "Transaction of R$" + String.format("%.2f", tx.getAmount()) + " exceeds high-value threshold",
            System.currentTimeMillis()
        );
    }

    public static FraudAlert transactionBurst(String accountId, long count, long windowStart, long windowEnd) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            accountId,
            "MEDIUM",
            "TRANSACTION_BURST",
            null,
            count + " transactions in 5-minute window",
            System.currentTimeMillis()
        );
    }

    public static FraudAlert unknownDevice(AuthEvent auth) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            auth.getUserId(),
            "MEDIUM",
            "UNKNOWN_DEVICE",
            auth.getUserId(),
            "Login from unknown device: " + auth.getDeviceId(),
            System.currentTimeMillis(),
            auth.getLatitude(),
            auth.getLongitude()
        );
    }

    public static FraudAlert passwordChange(AuthEvent auth) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            auth.getUserId(),
            "HIGH",
            "PASSWORD_CHANGE",
            auth.getUserId(),
            "Password changed by user " + auth.getUserId(),
            System.currentTimeMillis(),
            auth.getLatitude(),
            auth.getLongitude()
        );
    }

    public static FraudAlert accountTakeover(AuthEvent auth) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            auth.getUserId(),
            "CRITICAL",
            "ACCOUNT_TAKEOVER",
            auth.getUserId(),
            "Password change after " + auth.getRecentFailedAttempts() + " failed attempts",
            System.currentTimeMillis(),
            auth.getLatitude(),
            auth.getLongitude()
        );
    }

    public static FraudAlert emptyingAccount(String accountId, String userId, String description) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            accountId,
            "HIGH",
            "EMPTYING_ACCOUNT",
            userId,
            description,
            System.currentTimeMillis()
        );
    }

    public static FraudAlert parallelLogin(String userId, String description) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            userId,
            "HIGH",
            "PARALLEL_LOGIN",
            userId,
            description,
            System.currentTimeMillis()
        );
    }

    public static FraudAlert farawayLogin(AuthEvent auth, double distance, double speedKmh) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            auth.getUserId(),
            "HIGH",
            "FARAWAY_LOGIN",
            auth.getUserId(),
            String.format("Impossible travel: %.0f km at %.0f km/h", distance / 1000, speedKmh),
            System.currentTimeMillis(),
            auth.getLatitude(),
            auth.getLongitude()
        );
    }

    public static FraudAlert underObservation(String accountId, String userId, String description) {
        return new FraudAlert(
            UUID.randomUUID().toString(),
            accountId,
            "LOW",
            "UNDER_OBSERVATION",
            userId,
            description,
            System.currentTimeMillis()
        );
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
                ", latitude=" + latitude +
                ", longitude=" + longitude +
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
                Objects.equals(description, that.description) &&
                Objects.equals(latitude, that.latitude) &&
                Objects.equals(longitude, that.longitude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId, accountId, severity, alertType, userId, description, timestamp, latitude, longitude);
    }
}

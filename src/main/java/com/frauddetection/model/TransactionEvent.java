package com.frauddetection.model;

import java.util.Objects;

public class TransactionEvent {

    private String transactionId;
    private String accountId;
    private String userId;
    private String type;
    private double amount;
    private String originCountry;
    private String deviceId;
    private String ipAddress;
    private String merchantCategory;
    private String destinationAccount;
    private String status;

    public TransactionEvent() {}

    public TransactionEvent(
        String transactionId,
        String accountId,
        String userId,
        String type,
        double amount,
        String originCountry,
        String deviceId,
        String ipAddress,
        String merchantCategory,
        String destinationAccount,
        String status
    ) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.userId = userId;
        this.type = type;
        this.amount = amount;
        this.originCountry = originCountry;
        this.deviceId = deviceId;
        this.ipAddress = ipAddress;
        this.merchantCategory = merchantCategory;
        this.destinationAccount = destinationAccount;
        this.status = status;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getOriginCountry() {
        return originCountry;
    }

    public void setOriginCountry(String originCountry) {
        this.originCountry = originCountry;
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

    public String getMerchantCategory() {
        return merchantCategory;
    }

    public void setMerchantCategory(String merchantCategory) {
        this.merchantCategory = merchantCategory;
    }

    public String getDestinationAccount() {
        return destinationAccount;
    }

    public void setDestinationAccount(String destinationAccount) {
        this.destinationAccount = destinationAccount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return (
            "TransactionEvent{" +
            "transactionId='" +
            transactionId +
            '\'' +
            ", accountId='" +
            accountId +
            '\'' +
            ", userId='" +
            userId +
            '\'' +
            ", type='" +
            type +
            '\'' +
            ", amount=" +
            amount +
            ", originCountry='" +
            originCountry +
            '\'' +
            ", deviceId='" +
            deviceId +
            '\'' +
            ", ipAddress='" +
            ipAddress +
            '\'' +
            ", merchantCategory='" +
            merchantCategory +
            '\'' +
            ", destinationAccount='" +
            destinationAccount +
            '\'' +
            ", status='" +
            status +
            '\'' +
            '}'
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionEvent that = (TransactionEvent) o;
        return (
            Double.compare(that.amount, amount) == 0 &&
            Objects.equals(transactionId, that.transactionId) &&
            Objects.equals(accountId, that.accountId) &&
            Objects.equals(userId, that.userId) &&
            Objects.equals(type, that.type) &&
            Objects.equals(originCountry, that.originCountry) &&
            Objects.equals(deviceId, that.deviceId) &&
            Objects.equals(ipAddress, that.ipAddress) &&
            Objects.equals(merchantCategory, that.merchantCategory) &&
            Objects.equals(destinationAccount, that.destinationAccount) &&
            Objects.equals(status, that.status)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            transactionId,
            accountId,
            userId,
            type,
            amount,
            originCountry,
            deviceId,
            ipAddress,
            merchantCategory,
            destinationAccount,
            status
        );
    }
}

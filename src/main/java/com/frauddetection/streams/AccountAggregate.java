package com.frauddetection.streams;

import com.frauddetection.model.TransactionEvent;
import java.math.BigDecimal;

public class AccountAggregate {
    private BigDecimal totalAmount = BigDecimal.ZERO;
    private long count = 0;
    private String lastUserId;
    private String lastAccountId;

    public AccountAggregate() {}

    public AccountAggregate add(TransactionEvent tx) {
        this.totalAmount = this.totalAmount.subtract(BigDecimal.valueOf(tx.getAmount()));
        this.count++;
        this.lastUserId = tx.getUserId();
        this.lastAccountId = tx.getAccountId();
        return this;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount != null ? totalAmount : BigDecimal.ZERO;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getLastUserId() {
        return lastUserId;
    }

    public void setLastUserId(String lastUserId) {
        this.lastUserId = lastUserId;
    }

    public String getLastAccountId() {
        return lastAccountId;
    }

    public void setLastAccountId(String lastAccountId) {
        this.lastAccountId = lastAccountId;
    }
}

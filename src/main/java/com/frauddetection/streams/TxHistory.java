package com.frauddetection.streams;

import com.frauddetection.model.TransactionEvent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class TxHistory {
    private List<BigDecimal> amounts;

    public TxHistory() {
        this.amounts = new ArrayList<>();
    }

    public List<BigDecimal> getAmounts() {
        return amounts;
    }

    public void setAmounts(List<BigDecimal> amounts) {
        this.amounts = amounts != null ? amounts : new ArrayList<>();
    }

    public void add(TransactionEvent tx) {
        amounts.add(BigDecimal.valueOf(tx.getAmount()));
    }

    public BigDecimal sumLastN(int n) {
        int count = Math.min(n, amounts.size());
        BigDecimal sum = BigDecimal.ZERO;
        for (int i = amounts.size() - count; i < amounts.size(); i++) {
            sum = sum.add(amounts.get(i));
        }
        return sum;
    }

    public int size() {
        return amounts.size();
    }
}

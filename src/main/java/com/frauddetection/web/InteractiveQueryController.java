package com.frauddetection.web;

import com.frauddetection.streams.LastLogin;
import com.frauddetection.streams.StreamsManager;
import com.frauddetection.streams.TxHistory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin
@RequestMapping("/api/queries")
public class InteractiveQueryController {

    @Autowired
    private StreamsManager streamsManager;

    @GetMapping("/faraway-logins/{userId}")
    public LastLogin getFarawayLogin(@PathVariable String userId) {
        var store = streamsManager.<LastLogin>getStore("last-login-store");
        return store.get(userId);
    }

    @GetMapping("/observations")
    public List<Map<String, Object>> getObservations() {
        var store = streamsManager.<TxHistory>getStore("observation-store");
        List<Map<String, Object>> result = new ArrayList<>();
        try (var iter = store.all()) {
            iter.forEachRemaining(entry -> {
                if (entry.value.size() >= 5) {
                    result.add(Map.of(
                            "userId", entry.key,
                            "txCount", entry.value.size(),
                            "amounts", entry.value.getAmounts()));
                }
            });
        }
        return result;
    }
}

package com.frauddetection.streams;

import com.frauddetection.model.GeoLocation;

public class LastLogin {
    private Long timestamp;
    private GeoLocation geoLocation;

    public LastLogin() {}

    public LastLogin(Long timestamp, GeoLocation geoLocation) {
        this.timestamp = timestamp;
        this.geoLocation = geoLocation;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public GeoLocation getGeoLocation() {
        return geoLocation;
    }

    public void setGeoLocation(GeoLocation geoLocation) {
        this.geoLocation = geoLocation;
    }
}

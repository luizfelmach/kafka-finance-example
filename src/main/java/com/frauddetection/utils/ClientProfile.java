package com.frauddetection.utils;

import java.util.List;

public record ClientProfile(
    String userId,
    List<String> accounts,
    List<String> trustedDevices,
    String homeIp
) {}

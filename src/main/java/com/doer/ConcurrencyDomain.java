package com.doer;

import java.time.Duration;
import java.util.List;

public interface ConcurrencyDomain {
    String getName();

    int getValue();

    List<String> getStatuses();

    Duration getDelay(String status);

    Duration getRetryDelay(String status);
}

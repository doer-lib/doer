package com.doer;

import java.time.Duration;
import java.util.List;

interface ConcurrencyDomain {
    String getName();

    int getLimit();

    List<String> getStatuses();

    Duration getDelay(String status);

    Duration getRetryDelay(String status);
}

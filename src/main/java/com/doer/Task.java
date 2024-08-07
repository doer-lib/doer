package com.doer;

import java.time.Instant;

public class Task {
    private Long id;
    private String status;
    private boolean inProgress;
    private Instant created;
    private Instant modified;
    private Instant failingSince;
    private int version;

    public void assignFieldsFrom(Task other) {
        id = other.id;
        status = other.status;
        inProgress = other.inProgress;
        created = other.created;
        modified = other.modified;
        failingSince = other.failingSince;
        version = other.version;
    }

    public Long getId() {
        return id;
    }

    protected void setId(Long id) {
        this.id = id;
    }

    public boolean isInProgress() {
        return inProgress;
    }

    public void setInProgress(boolean inProgress) {
        this.inProgress = inProgress;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getCreated() {
        return created;
    }

    protected void setCreated(Instant created) {
        this.created = created;
    }

    public Instant getModified() {
        return modified;
    }

    protected void setModified(Instant modified) {
        this.modified = modified;
    }

    public Instant getFailingSince() {
        return failingSince;
    }

    public void setFailingSince(Instant failingSince) {
        this.failingSince = failingSince;
    }

    public int getVersion() {
        return version;
    }

    protected void setVersion(int version) {
        this.version = version;
    }
}

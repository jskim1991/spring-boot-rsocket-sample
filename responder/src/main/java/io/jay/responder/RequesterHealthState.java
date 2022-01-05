package io.jay.responder;

import lombok.Data;

@Data
public
class RequesterHealthState {
    public static final String STARTED = "started";
    public static final String STOPPED = "stopped";
    private final String state;

    public RequesterHealthState() {
        this.state = STARTED;
    }

    public RequesterHealthState(String s) {
        this.state = s;
    }
}

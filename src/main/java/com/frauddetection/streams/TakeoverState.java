package com.frauddetection.streams;

public class TakeoverState {
    private boolean loginSeen;
    private boolean pwChangeSeen;
    private long loginTimestamp;
    private long pwChangeTimestamp;

    public TakeoverState() {}

    public boolean isLoginSeen() { return loginSeen; }
    public void setLoginSeen(boolean loginSeen) { this.loginSeen = loginSeen; }

    public boolean isPwChangeSeen() { return pwChangeSeen; }
    public void setPwChangeSeen(boolean pwChangeSeen) { this.pwChangeSeen = pwChangeSeen; }

    public long getLoginTimestamp() { return loginTimestamp; }
    public void setLoginTimestamp(long loginTimestamp) { this.loginTimestamp = loginTimestamp; }

    public long getPwChangeTimestamp() { return pwChangeTimestamp; }
    public void setPwChangeTimestamp(long pwChangeTimestamp) { this.pwChangeTimestamp = pwChangeTimestamp; }
}

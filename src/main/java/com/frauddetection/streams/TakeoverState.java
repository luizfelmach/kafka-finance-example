package com.frauddetection.streams;

public class TakeoverState {
    private boolean loginSeen;
    private boolean pwChangeSeen;

    public TakeoverState() {}

    public boolean isLoginSeen() { return loginSeen; }
    public void setLoginSeen(boolean loginSeen) { this.loginSeen = loginSeen; }

    public boolean isPwChangeSeen() { return pwChangeSeen; }
    public void setPwChangeSeen(boolean pwChangeSeen) { this.pwChangeSeen = pwChangeSeen; }
}

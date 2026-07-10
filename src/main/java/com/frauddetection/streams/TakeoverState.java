package com.frauddetection.streams;

public class TakeoverState {
    private boolean loginSeen;
    private boolean pwChangeSeen;
    private boolean alertSent;

    public TakeoverState() {}

    public TakeoverState(boolean loginSeen, boolean pwChangeSeen, boolean alertSent) {
        this.loginSeen = loginSeen;
        this.pwChangeSeen = pwChangeSeen;
        this.alertSent = alertSent;
    }

    public boolean isLoginSeen() { return loginSeen; }
    public void setLoginSeen(boolean loginSeen) { this.loginSeen = loginSeen; }

    public boolean isPwChangeSeen() { return pwChangeSeen; }
    public void setPwChangeSeen(boolean pwChangeSeen) { this.pwChangeSeen = pwChangeSeen; }

    public boolean isAlertSent() { return alertSent; }
    public void setAlertSent(boolean alertSent) { this.alertSent = alertSent; }
}

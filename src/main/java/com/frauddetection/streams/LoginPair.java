package com.frauddetection.streams;

import com.frauddetection.model.AuthEvent;

public class LoginPair {
    private AuthEvent previous;
    private AuthEvent current;

    public LoginPair() {}

    public LoginPair(AuthEvent previous, AuthEvent current) {
        this.previous = previous;
        this.current = current;
    }

    public AuthEvent getPrevious() { return previous; }
    public void setPrevious(AuthEvent previous) { this.previous = previous; }

    public AuthEvent getCurrent() { return current; }
    public void setCurrent(AuthEvent current) { this.current = current; }
}

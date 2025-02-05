package jl95.pubsub.util;

public enum MessageType {

    PUBLISH                  (""),
    REQ_CLOSE                ("close"),
    REQ_SUBSCRIPTION_BY_LIST ("sub-list"),
    REQ_SUBSCRIPTION_BY_REGEX("sub-regex"),
    REQ_SUBSCRIPTION_TO_ALL  ("sub-all"),
    REQ_SUBSCRIPTION_TO_NONE ("sub-none");

    public final String serial;
    MessageType(String serial) {this.serial = serial;}
}

package jl95.net.pubsub.util;

public enum MessageType {

    MEMBER                   ("member"),
    PUBLISH                  ("pub"),
    REQ_CLOSE                ("close"),
    REQ_SUBSCRIPTION_BY_LIST ("sub-list"),
    REQ_SUBSCRIPTION_BY_REGEX("sub-regex"),
    REQ_SUBSCRIPTION_TO_ALL  ("sub-all"),
    REQ_SUBSCRIPTION_TO_NONE ("sub-none");

    public final String value;
    MessageType(String value) {this.value = value;}
}

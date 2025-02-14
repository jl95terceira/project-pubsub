package jl95.net.pubsub;

public interface Subscription {

    Boolean accepts(String topicName);
}

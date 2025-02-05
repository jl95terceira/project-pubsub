package jl95.pubsub;

public interface Subscription {

    Boolean accepts(String topicName);
}

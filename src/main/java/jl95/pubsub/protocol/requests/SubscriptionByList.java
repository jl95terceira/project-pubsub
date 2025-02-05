package jl95.pubsub.protocol.requests;

import static jl95.lang.SuperPowers.*;
import java.util.HashSet;
import java.util.Set;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.*;
import jl95.pubsub.Subscription;

public class SubscriptionByList
    extends NamedDataClass implements Subscription {

    public Set<String> topicNames = new HashSet<>();

    @Override public Boolean accepts(String topicName) {
        return topicNames.contains(topicName);
    }
    @Override public Iterable<Tuple2<String,?>> namedData       () {
        return I(
            tuple("topicNames", topicNames)
        );
    }
}

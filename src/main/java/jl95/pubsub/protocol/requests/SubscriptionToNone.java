package jl95.pubsub.protocol.requests;

import static jl95.lang.SuperPowers.*;

import jl95.pubsub.Subscription;
import jl95.lang.*;
import jl95.lang.variadic.*;

public class SubscriptionToNone
    extends NamedDataClass implements Subscription {

    @Override public    Boolean     accepts(String topicName) {
        return false;
    }
    @Override protected Iterable<Tuple2<String, ?>> namedData       () {
        return I();
    }
}

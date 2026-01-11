package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.*;

import jl95.net.pubsub.Subscription;
import jl95.lang.*;
import jl95.lang.variadic.*;
import jl95.util.NamedDataClass;

public class SubscriptionToNone
    extends NamedDataClass implements Subscription {

    @Override public    Boolean     accepts(String topicName) {
        return false;
    }
    @Override protected Iterable<Tuple2<String, ?>> namedData       () {
        return I();
    }
}

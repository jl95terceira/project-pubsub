package jl95.pubsub.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.tuple;

import java.util.regex.Pattern;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.Tuple2;
import jl95.pubsub.Subscription;

public class SubscriptionByRegex
    extends NamedDataClass implements Subscription {

    public Pattern topicPattern = Pattern.compile("");

    @Override public Boolean accepts(String topicName) { return topicPattern.matcher(topicName).matches(); }
    @Override public Iterable<Tuple2<String,?>> namedData        () {
        return I(
            tuple("topicPattern", topicPattern)
        );
    }
}

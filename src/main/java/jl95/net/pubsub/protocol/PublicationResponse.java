package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.tuple;

import jl95.util.NamedDataClass;
import jl95.lang.variadic.Tuple2;

public class PublicationResponse extends NamedDataClass {

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I();
    }
}

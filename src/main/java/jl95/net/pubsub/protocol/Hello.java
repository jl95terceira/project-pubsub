package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.tuple;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.Tuple2;

public class Hello extends NamedDataClass {

    public enum Type {
        MEMBER_REQUESTS,
        MEMBER_RESPONSES,
        BROKER_REQUESTS,
        BROKER_RESPONSES;
    }

    public Type type;

    public Hello() {this(Type.MEMBER_REQUESTS);}
    public Hello(Type type) {this.type = type;}

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("type", type)
        );
    }
}

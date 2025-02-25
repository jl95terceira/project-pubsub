package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.tuple;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.Tuple2;

public class Hello extends NamedDataClass {

    public enum Type {
        MEMBER_REQUESTING_FROM_BROKER,
        MEMBER_RESPONDING_TO_BROKER,
//        BROKER_REQUESTING_FROM_BROKER,
//        BROKER_RESPONDING_TO_BROKER;
    }

    public Type type;

    public Hello() {this(Type.MEMBER_REQUESTING_FROM_BROKER);}
    public Hello(Type type) {this.type = type;}

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("type", type)
        );
    }
}

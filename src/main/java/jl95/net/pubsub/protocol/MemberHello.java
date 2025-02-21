package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.tuple;

import jl95.lang.NamedDataClass;
import jl95.lang.variadic.Tuple2;

public class MemberHello extends NamedDataClass {

    public enum Type {
        REQUEST_FROM_BROKER,
        RESPOND_TO_BROKER;
    }

    public Type type;

    public MemberHello() {this(Type.REQUEST_FROM_BROKER);}
    public MemberHello(Type type) {this.type = type;}

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("type", type)
        );
    }
}

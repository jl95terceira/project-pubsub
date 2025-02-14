package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.*;

import jl95.lang.*;
import jl95.lang.variadic.*;

public class Publication extends NamedDataClass {

    public String topicName;
    public byte[] data;

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("topic", topicName),
            tuple("data" , data)
        );
    }
}

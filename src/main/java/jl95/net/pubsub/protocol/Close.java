package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.*;

import jl95.lang.*;
import jl95.lang.variadic.*;

public class Close
    extends NamedDataClass {

    @Override protected Iterable<Tuple2<String, ?>> namedData() {
        return I();
    }
}

package jl95.pubsub;

import java.net.InetSocketAddress;
import java.util.*;

import static jl95.lang.SuperPowers.*;
import jl95.lang.*;
import jl95.lang.variadic.Tuple2;

public class Message<B> extends NamedDataClass {

    public UUID id          = UUID.randomUUID();
    public B    body        = null;
    public UUID clientId    = null;
    public Set<InetSocketAddress> stamps = Set();

    @Override protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("id"      , id),
            tuple("body"    , body),
            tuple("clientId", clientId),
            tuple("stamps"  , stamps)
        );
    }
}

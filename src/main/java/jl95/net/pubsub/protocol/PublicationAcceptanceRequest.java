package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.tuple;

import java.util.UUID;

import javax.json.JsonValue;

import jl95.util.NamedDataClass;
import jl95.lang.variadic.Tuple2;

public class PublicationAcceptanceRequest extends NamedDataClass {

    public UUID id;

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("id", id)
        );
    }
}

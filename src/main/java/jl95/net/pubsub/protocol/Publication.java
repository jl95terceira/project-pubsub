package jl95.net.pubsub.protocol;

import static jl95.lang.SuperPowers.*;

import java.util.UUID;

import javax.json.JsonValue;

import jl95.lang.*;
import jl95.lang.variadic.*;

public class Publication extends NamedDataClass {

    public UUID      id = UUID.randomUUID();
    public String    topicName;
    public JsonValue data;

    @Override
    protected Iterable<Tuple2<String, ?>> namedData() {
        return I(
            tuple("id"   , id),
            tuple("topic", topicName),
            tuple("data" , data)
        );
    }
}

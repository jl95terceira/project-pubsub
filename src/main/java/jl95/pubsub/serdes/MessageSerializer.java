package jl95.pubsub.serdes;

import static jl95.lang.SuperPowers.*;

import javax.json.*;

import jl95.pubsub.protocol.Message;
import jl95.lang.variadic.*;

public class MessageSerializer {

    public enum Id {

        ID  ("id"),
        TYPE("type"),
        BODY("body");

        public final String value;
        Id(String value) {this.value = value;}
    }

    /**
     * get function to serialize request sub-types
     * @param typeName name of type (MUST be unique for the given request body type)
     * @param bodySerializer request body serializer
     * @return request serializer function
     * @param <B> request body type
     */
    public static <B> Function1<JsonValue, Message<B>> get(String                    typeName,
                                                           Function1<JsonValue, B>   bodySerializer) {
        return req -> {
            var job = Json.createObjectBuilder();
            for (var t: I(
                tuple(Id.ID  , Json.createValue(req.id  .toString())),
                tuple(Id.TYPE, Json.createValue(typeName.toString())),
                tuple(Id.BODY, bodySerializer.call(req.body))
            )) {
                job.add(t.a1.value, t.a2);
            }
            return job.build();
        };
    }
}

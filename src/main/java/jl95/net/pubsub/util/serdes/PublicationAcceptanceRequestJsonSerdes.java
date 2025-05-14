package jl95.net.pubsub.util.serdes;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.method;
import static jl95.lang.SuperPowers.tuple;

import java.util.UUID;

import javax.json.Json;
import javax.json.JsonValue;

import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.PublicationAcceptanceRequest;
import jl95.net.pubsub.util.SerdesDefaults;

public class PublicationAcceptanceRequestJsonSerdes {
    public enum Id {

        ID("id");

        public final String value;
        Id(String value) {this.value = value;}
    }

    public static JsonValue                    toJson  (PublicationAcceptanceRequest req) {

        var job = Json.createObjectBuilder();
        for (var t: I(

            tuple(Id.ID, SerdesDefaults.stringToJson.apply(req.id.toString()))

        ).map(t -> tuple(t.a1.value, t.a2))) {
            job.add(t.a1, t.a2);
        }
        return job.build();
    }
    public static PublicationAcceptanceRequest fromJson(JsonValue                    reqjson) {

        var jo = reqjson.asJsonObject();
        var x = new PublicationAcceptanceRequest();
        for (var t: I(

            tuple(Id.ID, method((String i) -> { x.id = UUID.fromString(SerdesDefaults.stringFromJson.apply(jo.get(i))); }))

        )) {
                t.a2.call(t.a1.value);
        }
        return x;
    }
}

package jl95.rpc.util.serdes;

import static jl95.lang.SuperPowers.*;

import java.util.UUID;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import jl95.rpc.util.Request;
import jl95.rpc.util.SerdesDefaults;

public class RequestJsonSerdes {

    public enum Id {

    ID     ("id"),
    PAYLOAD("payload");

    public final String value;
    Id(String value) {this.value = value;}
    }

    public static JsonObject toJson  (Request   req) {

        var job = Json.createObjectBuilder();
        for (var t: I(

            tuple(Id.ID,      SerdesDefaults.stringToJson.call                                  (req.id.toString())),
            tuple(Id.PAYLOAD, SerdesDefaults.stringToJson.call(SerdesDefaults.bytesToString.call(req.payload)))

        ).map(t -> tuple(t.a1.value, t.a2))) {
            job.add(t.a1, t.a2);
        }
        return job.build();
    }
    public static Request    fromJson(JsonValue reqjson) {

        var jo = reqjson.asJsonObject();
        var x = new Request();
        for (var t: I(

            tuple(Id.ID,      method((String i) -> { x.id      = UUID.fromString                    (SerdesDefaults.stringFromJson.call(jo.get(i))); })),
            tuple(Id.PAYLOAD, method((String i) -> { x.payload = SerdesDefaults.bytesFromString.call(SerdesDefaults.stringFromJson.call(jo.get(i))); }))

        )) {
                t.a2.call(t.a1.value);
        }
        return x;
    }
}

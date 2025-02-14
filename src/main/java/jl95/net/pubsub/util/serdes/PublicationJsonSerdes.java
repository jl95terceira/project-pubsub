package jl95.net.pubsub.util.serdes;

import static jl95.lang.SuperPowers.*;

import javax.json.Json;
import javax.json.JsonValue;

import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.SerdesDefaults;

public class PublicationJsonSerdes {
    public enum Id {

        TOPIC("topic"),
        DATA ("data");

        public final String value;
        Id(String value) {this.value = value;}
    }

    public static JsonValue   toJson  (Publication req) {

        var job = Json.createObjectBuilder();
        for (var t: I(

            tuple(Id.TOPIC, SerdesDefaults.stringToJson.call                                  (req.topicName)),
            tuple(Id.DATA , SerdesDefaults.stringToJson.call(SerdesDefaults.bytesToString.call(req.data)))

        ).map(t -> tuple(t.a1.value, t.a2))) {
            job.add(t.a1, t.a2);
        }
        return job.build();
    }
    public static Publication fromJson(JsonValue   reqjson) {

        var jo = reqjson.asJsonObject();
        var x = new Publication();
        for (var t: I(

            tuple(Id.TOPIC, method((String i) -> { x.topicName =                                      SerdesDefaults.stringFromJson.apply(jo.get(i)); })),
            tuple(Id.DATA , method((String i) -> { x.data      = SerdesDefaults.bytesFromString.apply(SerdesDefaults.stringFromJson.apply(jo.get(i))); }))

        )) {
                t.a2.call(t.a1.value);
        }
        return x;
    }
}

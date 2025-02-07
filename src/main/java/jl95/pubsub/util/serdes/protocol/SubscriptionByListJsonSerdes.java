package jl95.pubsub.util.serdes.protocol;

import static jl95.lang.SuperPowers.*;
import javax.json.*;

import jl95.pubsub.protocol.SubscriptionByList;
import jl95.pubsub.util.SerdesDefaults;

public class SubscriptionByListJsonSerdes {

    public enum Id {

    TOPIC_NAMES("topicNames");

    public final String value;
    Id(String value) {this.value = value;}
    }

    public static JsonObject         toJson  (SubscriptionByList req) {

        var job = Json.createObjectBuilder();
        for (var t: I(

            tuple(Id.TOPIC_NAMES, SerdesDefaults.listOfStringToJson.call(req.topicNames))

        ).map(t -> tuple(t.a1.value, t.a2))) {
            job.add(t.a1, t.a2);
        }
        return job.build();
    }
    public static SubscriptionByList fromJson(JsonValue          reqjson) {

        var jo = reqjson.asJsonObject();
        var x = new SubscriptionByList();
        for (var t: I(

            tuple(Id.TOPIC_NAMES, method((String i) -> { x.topicNames.addAll(SerdesDefaults.listOfStringFromJson.call(jo.get(i))); }))

        )) {
                t.a2.call(t.a1.value);
        }
        return x;
    }
}

package jl95.net.pubsub.util.serdes.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.method;
import static jl95.lang.SuperPowers.tuple;

import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.util.SerdesDefaults;

public class SubscriptionByRegexJsonSerdes {

    public enum Id {

    TOPIC_REGEX("topicRegex");

    public final String value;
    Id(String value) {this.value = value;}
    }

    public static JsonObject          toJson  (SubscriptionByRegex req) {

        var job = Json.createObjectBuilder();
        for (var t: I(

            tuple(Id.TOPIC_REGEX, SerdesDefaults.stringToJson.call(req.topicPattern.pattern()))

        ).map(t -> tuple(t.a1.value, t.a2))) {
            job.add(t.a1, t.a2);
        }
        return job.build();
    }
    public static SubscriptionByRegex fromJson(JsonValue           reqjson) {

        var jo = reqjson.asJsonObject();
        var x = new SubscriptionByRegex();
        for (var t: I(

            tuple(Id.TOPIC_REGEX, method((String i) -> { x.topicPattern = Pattern.compile(SerdesDefaults.stringFromJson.call(jo.get(i))); }))

        )) {
                t.a2.call(t.a1.value);
        }
        return x;
    }
}

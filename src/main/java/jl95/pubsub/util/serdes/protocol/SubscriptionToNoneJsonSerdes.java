package jl95.pubsub.util.serdes.protocol;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import jl95.pubsub.protocol.SubscriptionToNone;

public class SubscriptionToNoneJsonSerdes {

    public static JsonObject         toJson  (SubscriptionToNone req) {

        return Json.createObjectBuilder().build();
    }
    public static SubscriptionToNone fromJson(JsonValue          reqjson) {

        return new SubscriptionToNone();
    }
}

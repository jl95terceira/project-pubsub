package jl95.pubsub.util.serdes.protocol;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import jl95.pubsub.protocol.SubscriptionToAll;

public class SubscriptionToAllJsonSerdes {

    public static JsonObject              toJson  (SubscriptionToAll req) {

        return Json.createObjectBuilder().build();
    }
    public static SubscriptionToAll fromJson(JsonValue               reqjson) {

        return new SubscriptionToAll();
    }
}

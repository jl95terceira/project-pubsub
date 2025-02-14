package jl95.net.pubsub.util.serdes.protocol;

import javax.json.*;

import jl95.net.pubsub.protocol.Close;

public class CloseJsonSerdes {

    public static JsonValue toJson  (Close req) {

        return Json.createObjectBuilder().build();
    }
    public static Close     fromJson(JsonValue reqjson) {

        return new Close();
    }
}

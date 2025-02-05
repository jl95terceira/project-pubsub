package jl95.pubsub.serdes.requests;

import javax.json.*;

import jl95.pubsub.protocol.requests.Close;

public class CloseJsonSerdes {

    public static JsonValue toJson  (Close     req) {

        return Json.createObjectBuilder().build();
    }
    public static Close     fromJson(JsonValue reqjson) {

        return new Close();
    }
}

package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Io;
import jl95.rpc.util.SerdesDefaults;

public class JsonRequesterFactory {

    public static RequesterIf<JsonValue, JsonValue> get(Io io,
                                                      Requester.SendOptions options) {

        return StringRequesterFactory.get(io, options).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
}

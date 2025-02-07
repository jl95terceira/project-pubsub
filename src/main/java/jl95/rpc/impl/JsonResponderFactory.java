package jl95.rpc.impl;

import javax.json.JsonValue;

import jl95.net.Io;
import jl95.rpc.ResponderIf;
import jl95.rpc.util.SerdesDefaults;

public class JsonResponderFactory {

    public static ResponderIf<JsonValue, JsonValue> get(Io io) {

        return StringResponderFactory.get(io).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
}

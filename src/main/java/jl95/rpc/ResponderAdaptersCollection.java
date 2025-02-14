package jl95.rpc;

import javax.json.JsonValue;

import jl95.rpc.util.SerdesDefaults;

public class ResponderAdaptersCollection {

    public static ResponderIf<String,    String>    getStringResponder(ResponderIf<byte[], byte[]> responder) {

        return responder.adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }
    public static ResponderIf<String,    Void>      getStringGetter   (ResponderIf<byte[], byte[]> responder) {

        return responder.adapted(
            SerdesDefaults.stringFromBytes,
            x -> new byte[1]
        );
    }
    public static ResponderIf<JsonValue, JsonValue> getJsonResponder  (ResponderIf<byte[], byte[]> responder) {

        return getStringResponder(responder).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
    public static ResponderIf<JsonValue, Void>      getJsonGetter     (ResponderIf<byte[], byte[]> responder) {

        return getStringGetter(responder).adapted(
            SerdesDefaults.jsonFromString,
            x -> x
        );
    }
    public static TypeSwitchedResponderIf<String,    String>    getTsStringResponder(TypeSwitchedResponderIf<byte[], byte[]> responder) {
        return responder.adapted(SerdesDefaults.stringFromBytes, SerdesDefaults.stringToBytes);
    }
    public static TypeSwitchedResponderIf<JsonValue, JsonValue> getTsJsonResponder  (TypeSwitchedResponderIf<byte[], byte[]> responder) {
        return getTsStringResponder(responder).adapted(SerdesDefaults.jsonFromString, SerdesDefaults.jsonToString);
    }
}

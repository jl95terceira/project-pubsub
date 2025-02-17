package jl95.net.rpc.collections;

import javax.json.JsonValue;

import jl95.net.rpc.ResponderIf;
import jl95.net.rpc.switched.TypeSwitchedResponderIf;
import jl95.net.rpc.util.SerdesDefaults;

public class ResponderAdaptersCollection {

    public static ResponderIf<String,    String> asStringResponder(ResponderIf<byte[], byte[]> responder) {

        return responder.adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }
    public static ResponderIf<String,    Void> asStringGetter(ResponderIf<byte[], byte[]> responder) {

        return responder.adapted(
            SerdesDefaults.stringFromBytes,
            x -> new byte[1]
        );
    }
    public static ResponderIf<JsonValue, JsonValue> asJsonResponder(ResponderIf<byte[], byte[]> responder) {

        return asStringResponder(responder).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
    public static ResponderIf<JsonValue, Void> asJsonGetter(ResponderIf<byte[], byte[]> responder) {

        return asStringGetter(responder).adapted(
            SerdesDefaults.jsonFromString,
            x -> x
        );
    }
    public static TypeSwitchedResponderIf<String,    String> asTsStringResponder(TypeSwitchedResponderIf<byte[], byte[]> responder) {
        return responder.adapted(SerdesDefaults.stringFromBytes, SerdesDefaults.stringToBytes);
    }
    public static TypeSwitchedResponderIf<JsonValue, JsonValue> asTsJsonResponder(TypeSwitchedResponderIf<byte[], byte[]> responder) {
        return asTsStringResponder(responder).adapted(SerdesDefaults.jsonFromString, SerdesDefaults.jsonToString);
    }
}

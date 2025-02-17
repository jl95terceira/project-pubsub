package jl95.net.rpc.collections;

import javax.json.JsonValue;

import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.switched.TypedRequesterIf;
import jl95.net.rpc.util.SerdesDefaults;

public class RequesterAdaptersCollection {

    public static RequesterIf<String,    String>    asStringPostGetRequester(RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
    public static RequesterIf<String,    Void>      asStringPostRequester   (RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            SerdesDefaults.stringToBytes,
            x -> null
        );
    }
    public static RequesterIf<Void,      String>    asStringGetRequester    (RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            x -> new byte[1],
            SerdesDefaults.stringFromBytes
        );
    }
    public static RequesterIf<JsonValue, JsonValue> asJsonPostGetRequester  (RequesterIf<byte[], byte[]> requester) {

        return asStringPostGetRequester(requester).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
    public static RequesterIf<JsonValue, Void>      asJsonPostRequester     (RequesterIf<byte[], byte[]> requester) {

        return asStringPostRequester(requester).adapted(
            SerdesDefaults.jsonToString,
            x -> x
        );
    }
    public static RequesterIf<Void, JsonValue>      asJsonGetRequester      (RequesterIf<byte[], byte[]> requester) {

        return asStringGetRequester(requester).adapted(
            x -> x,
            SerdesDefaults.jsonFromString
        );
    }

    public static TypedRequesterIf<String,    String>    asTypedStringRequester(TypedRequesterIf<byte[], byte[]> requester) {
        return requester.adapted(SerdesDefaults.stringToBytes, SerdesDefaults.stringFromBytes);
    }
    public static TypedRequesterIf<JsonValue, JsonValue> asTypedJsonRequester  (TypedRequesterIf<byte[], byte[]> requester) {
        return asTypedStringRequester(requester).adapted(SerdesDefaults.jsonToString, SerdesDefaults.jsonFromString);
    }
}

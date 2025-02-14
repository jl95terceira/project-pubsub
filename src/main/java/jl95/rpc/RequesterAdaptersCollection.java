package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Ios;
import jl95.rpc.util.SerdesDefaults;

public class RequesterAdaptersCollection {

    public static RequesterIf     <String,    String>    getStringRequester     (RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
    public static RequesterIf     <String,    Void>      getStringPutter        (RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            SerdesDefaults.stringToBytes,
            x -> null
        );
    }
    public static RequesterIf     <JsonValue, JsonValue> getJsonRequester       (RequesterIf<byte[], byte[]> requester) {

        return getStringRequester(requester).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
    public static RequesterIf     <JsonValue, Void>      getJsonPutter          (RequesterIf<byte[], byte[]> requester) {

        return getStringPutter(requester).adapted(
            SerdesDefaults.jsonToString,
            x -> null
        );
    }
    public static TypedRequesterIf<String,    String>    getTypedStringRequester(TypedRequesterIf<byte[], byte[]> requester) {
        return requester.adapted(SerdesDefaults.stringToBytes, SerdesDefaults.stringFromBytes);
    }
    public static TypedRequesterIf<JsonValue, JsonValue> getTypedJsonRequester  (TypedRequesterIf<byte[], byte[]> requester) {
        return getTypedStringRequester(requester).adapted(SerdesDefaults.jsonToString, SerdesDefaults.jsonFromString);
    }
}

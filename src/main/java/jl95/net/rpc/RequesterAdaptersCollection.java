package jl95.net.rpc;

import javax.json.JsonValue;

import jl95.net.rpc.ts.TypedRequesterIf;
import jl95.net.rpc.util.SerdesDefaults;

public class RequesterAdaptersCollection {

    public static RequesterIf     <String,    String> asStringRequester(RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
    public static RequesterIf     <String,    Void> asStringPutter(RequesterIf<byte[], byte[]> requester) {

        return requester.adapted(
            SerdesDefaults.stringToBytes,
            x -> null
        );
    }
    public static RequesterIf     <JsonValue, JsonValue> asJsonRequester(RequesterIf<byte[], byte[]> requester) {

        return asStringRequester(requester).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
    public static RequesterIf     <JsonValue, Void> asJsonPutter(RequesterIf<byte[], byte[]> requester) {

        return asStringPutter(requester).adapted(
            SerdesDefaults.jsonToString,
            x -> null
        );
    }
    public static TypedRequesterIf<String,    String> asTypedStringRequester(TypedRequesterIf<byte[], byte[]> requester) {
        return requester.adapted(SerdesDefaults.stringToBytes, SerdesDefaults.stringFromBytes);
    }
    public static TypedRequesterIf<JsonValue, JsonValue> asTypedJsonRequester(TypedRequesterIf<byte[], byte[]> requester) {
        return asTypedStringRequester(requester).adapted(SerdesDefaults.jsonToString, SerdesDefaults.jsonFromString);
    }
}

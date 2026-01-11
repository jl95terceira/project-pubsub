package jl95.net.rpc.collections;

import static jl95.lang.SuperPowers.self;

import javax.json.JsonValue;

import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.rpc.switched.TypedRequesterIf;

public class TypedRequesterAdaptersCollection {

    public static TypedRequesterIf<JsonValue, Void>      asPostRequester      (TypedRequesterIf<JsonValue, JsonValue> requester) {
        return requester.adapted(
            self::apply,
            x -> null
        );
    }
    public static TypedRequesterIf<Void,      JsonValue> asGetRequester       (TypedRequesterIf<JsonValue, JsonValue> requester) {

        return requester.adapted(
            x -> JsonValue.NULL,
            self::apply
        );
    }
    public static TypedRequesterIf<String,    String>    asStringRequester    (TypedRequesterIf<JsonValue, JsonValue> requester) {

        return requester.adapted(
            SerdesDefaults.stringToJson,
            SerdesDefaults.stringFromJson
        );
    }
    public static TypedRequesterIf<String,    Void>      asStringPostRequester(TypedRequesterIf<JsonValue, JsonValue> requester) {

        return asPostRequester(requester).adapted(
            SerdesDefaults.stringToJson,
            x -> x
        );
    }
    public static TypedRequesterIf<Void,      String>    asStringGetRequester (TypedRequesterIf<JsonValue, JsonValue> requester) {

        return asGetRequester(requester).adapted(
            x -> x,
            SerdesDefaults.stringFromJson
        );
    }
    public static TypedRequesterIf<byte[],    byte[]>    asBytesRequester     (TypedRequesterIf<JsonValue, JsonValue> requester) {

        return asStringRequester(requester).adapted(
            SerdesDefaults.bytesToString,
            SerdesDefaults.bytesFromString
        );
    }
    public static TypedRequesterIf<byte[],    Void>      asBytesPostRequester (TypedRequesterIf<JsonValue, JsonValue> requester) {

        return asStringPostRequester(requester).adapted(
            SerdesDefaults.bytesToString,
            x -> x
        );
    }
    public static TypedRequesterIf<Void,      byte[]>    asBytesGetRequester  (TypedRequesterIf<JsonValue, JsonValue> requester) {

        return asStringGetRequester(requester).adapted(
            x -> x,
            SerdesDefaults.bytesFromString
        );
    }
}

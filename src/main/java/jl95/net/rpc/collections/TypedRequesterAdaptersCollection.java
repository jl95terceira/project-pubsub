package jl95.net.rpc.collections;

import static jl95.lang.SuperPowers.self;

import javax.json.JsonValue;

import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.rpc.switched.TypedRequester;

public class TypedRequesterAdaptersCollection {

    public static TypedRequester<JsonValue, Void> asPostRequester      (TypedRequester<JsonValue, JsonValue> requester) {
        return requester.adapted(
            self::apply,
            x -> null
        );
    }
    public static TypedRequester<Void,      JsonValue> asGetRequester       (TypedRequester<JsonValue, JsonValue> requester) {

        return requester.adapted(
            x -> JsonValue.NULL,
            self::apply
        );
    }
    public static TypedRequester<String,    String> asStringRequester    (TypedRequester<JsonValue, JsonValue> requester) {

        return requester.adapted(
            SerdesDefaults.stringToJson,
            SerdesDefaults.stringFromJson
        );
    }
    public static TypedRequester<String,    Void> asStringPostRequester(TypedRequester<JsonValue, JsonValue> requester) {

        return asPostRequester(requester).adapted(
            SerdesDefaults.stringToJson,
            x -> x
        );
    }
    public static TypedRequester<Void,      String> asStringGetRequester (TypedRequester<JsonValue, JsonValue> requester) {

        return asGetRequester(requester).adapted(
            x -> x,
            SerdesDefaults.stringFromJson
        );
    }
    public static TypedRequester<byte[],    byte[]> asBytesRequester     (TypedRequester<JsonValue, JsonValue> requester) {

        return asStringRequester(requester).adapted(
            SerdesDefaults.bytesToString,
            SerdesDefaults.bytesFromString
        );
    }
    public static TypedRequester<byte[],    Void> asBytesPostRequester (TypedRequester<JsonValue, JsonValue> requester) {

        return asStringPostRequester(requester).adapted(
            SerdesDefaults.bytesToString,
            x -> x
        );
    }
    public static TypedRequester<Void,      byte[]> asBytesGetRequester  (TypedRequester<JsonValue, JsonValue> requester) {

        return asStringGetRequester(requester).adapted(
            x -> x,
            SerdesDefaults.bytesFromString
        );
    }
}

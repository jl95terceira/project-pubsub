package jl95.net.rpc.collections;

import static jl95.lang.SuperPowers.self;

import javax.json.JsonValue;

import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.rpc.switched.TypeSwitchedResponder;

public class TypeSwitchedResponderAdaptersCollection {

    public static TypeSwitchedResponder<JsonValue, Void> asPostResponder      (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return responder.adapted(
            self::apply,
            x -> JsonValue.NULL
        );
    }
    public static TypeSwitchedResponder<Void,      JsonValue> asGetResponder       (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return responder.adapted(
            x -> null,
            self::apply
        );
    }
    public static TypeSwitchedResponder<String,    String> asStringResponder    (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return responder.adapted(
            SerdesDefaults.stringFromJson,
            SerdesDefaults.stringToJson
        );
    }
    public static TypeSwitchedResponder<String,    Void> asStringPostResponder(TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return asPostResponder(responder).adapted(
            SerdesDefaults.stringFromJson,
            x -> x
        );
    }
    public static TypeSwitchedResponder<Void,      String> asStringGetResponder (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return asGetResponder(responder).adapted(
            x -> x,
            SerdesDefaults.stringToJson
        );
    }
    public static TypeSwitchedResponder<byte[],    byte[]> asBytesResponder     (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return asStringResponder(responder).adapted(
            SerdesDefaults.bytesFromString,
            SerdesDefaults.bytesToString
        );
    }
    public static TypeSwitchedResponder<byte[],    Void> asBytesPostResponder (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return asStringPostResponder(responder).adapted(
            SerdesDefaults.bytesFromString,
            x -> x
        );
    }
    public static TypeSwitchedResponder<Void,      byte[]> asBytesGetResponder  (TypeSwitchedResponder<JsonValue, JsonValue> responder) {

        return asStringGetResponder(responder).adapted(
            x -> x,
            SerdesDefaults.bytesToString
        );
    }
}

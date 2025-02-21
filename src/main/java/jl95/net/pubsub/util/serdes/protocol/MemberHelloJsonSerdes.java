package jl95.net.pubsub.util.serdes.protocol;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.method;
import static jl95.lang.SuperPowers.tuple;

import javax.json.Json;
import javax.json.JsonValue;

import jl95.net.pubsub.Member;
import jl95.net.pubsub.protocol.MemberHello;

public class MemberHelloJsonSerdes {

    public enum Id {
        TYPE("type");
        public final String value;
        Id(String value) {this.value = value;}
    }

    public static JsonValue   toJson  (MemberHello x) {

        var jsono = Json.createObjectBuilder();
        for (var t: I(
            tuple(Id.TYPE, method((String i) -> { jsono.add(i, x.type.toString()); }))
        )) {
            t.a2.accept(t.a1.value);
        }
        return jsono.build();
    }
    public static MemberHello fromJson(JsonValue   json) {

        var jsono = json.asJsonObject();
        var x = new MemberHello();
        for (var t: I(
            tuple(Id.TYPE, method((String i) -> { x.type = MemberHello.Type.valueOf(jsono.getString(i)); }))
        )) {
            t.a2.accept(t.a1.value);
        }
        return x;
    }
}

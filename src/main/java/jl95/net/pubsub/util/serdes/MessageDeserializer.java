package jl95.net.pubsub.util.serdes;

import static jl95.lang.SuperPowers.*;

import java.util.UUID;

import javax.json.JsonValue;

import jl95.lang.I;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.lang.variadic.Function1;
import jl95.serdes.InetSocketAddressFromJson;
import jl95.serdes.ListFromJson;

public class MessageDeserializer {

    public static <B> Function1<Message<B>, JsonValue> get(Function1<B, JsonValue> bodyDeserializer) {
        return json -> {

            var jsono = json.asJsonObject();
            var req = new Message<B>();
            for (var t : I(

                tuple(MessageSerializer.Id.ID,        method((String i) -> {
                    req.id       = UUID.fromString(SerdesDefaults.stringFromJson.apply(jsono.get(i)));
                })),
                tuple(MessageSerializer.Id.BODY,      method((String i) -> {
                    req.body     = bodyDeserializer.apply(jsono.get(i));
                })),
                tuple(MessageSerializer.Id.CLIENT_ID, method((String i) -> {
                    req.memberId = UUID.fromString(SerdesDefaults.stringFromJson.apply(jsono.get(i)));
                })),
                tuple(MessageSerializer.Id.STAMPS,    method((String i) -> {
                    req.stamps   = I.of(ListFromJson.get(InetSocketAddressFromJson.get()).apply(jsono.get(i))).toSet();
                }))

            )) {
                t.a2.accept(t.a1.value);
            }
            return req;
        };
    }
}

package jl95.net.pubsub.util.serdes;

import static jl95.lang.SuperPowers.*;

import java.util.Map;
import java.util.UUID;

import javax.json.JsonValue;

import jl95.lang.I;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.lang.variadic.Function1;
import jl95.serdes.InetSocketAddressFromJson;
import jl95.serdes.ListFromJson;

public class MessageSwitchedDeserializer<R> implements Function1<R, JsonValue> {

    public static class DefaultCaseNotSetException extends RuntimeException {}

    private Map<String, Function1<R, JsonValue>> callbacksMap     = Map();
    private Function1<R, JsonValue>              callbacksDefault = (json) -> { throw new DefaultCaseNotSetException(); };

    private <B> Function1<R, JsonValue> getCaller(Function1<B, JsonValue> bodyDeserializer, Function1<R, Message<B>> callback) {
        return json -> {

            var jsono = json.asJsonObject();
            var req = new Message<B>();
            for (var t : I(

                tuple(MessageSerializer.Id.ID,              method((String i) -> {
                    req.id             = UUID.fromString(SerdesDefaults.stringFromJson.apply(jsono.get(i)));
                })),
                tuple(MessageSerializer.Id.BODY,            method((String i) -> {
                    req.body           = bodyDeserializer.apply(jsono.get(i));
                })),
                tuple(MessageSerializer.Id.CLIENT_ID,       method((String i) -> {
                    req.memberId = UUID.fromString(SerdesDefaults.stringFromJson.apply(jsono.get(i)));
                })),
                tuple(MessageSerializer.Id.STAMPS,          method((String i) -> {
                    req.stamps = I.of(ListFromJson.get(InetSocketAddressFromJson.get()).apply(jsono.get(i))).toSet();
                }))

            )) {
                t.a2.accept(t.a1.value);
            }
            return callback.apply(req);
        };
    }

    public final <B> void addCase   (String type, Function1<B, JsonValue> bodyDeserializer, Function1<R, Message<B>> callback) {
        callbacksMap.put(type, getCaller(bodyDeserializer, callback));
    }
    public final <B> void setDefault(             Function1<B, JsonValue> bodyDeserializer, Function1<R, Message<B>> callback) {
        callbacksDefault = getCaller(bodyDeserializer, callback);
    }

    @Override
    public R apply(JsonValue json) {
        String type = SerdesDefaults.stringFromJson.apply(json.asJsonObject().get(MessageSerializer.Id.TYPE.value));
        if (!callbacksMap.containsKey(type)) {
            return callbacksDefault.apply(json);
        }
        return callbacksMap.get(type).apply(json);
    }
}

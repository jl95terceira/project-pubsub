package jl95.pubsub.serdes;

import static jl95.lang.SuperPowers.*;

import java.util.Map;
import java.util.UUID;

import javax.json.JsonValue;

import jl95.pubsub.protocol.Message;
import jl95.pubsub.util.SerdesDefaults;
import jl95.lang.variadic.Function1;

public class MessageSwitchingDeserializer<R> implements Function1<R, JsonValue> {

    public static class DefaultCaseNotSetException extends RuntimeException {}

    private Map<String, Function1<R, JsonValue>> callbacksMap     = Map();
    private Function1<R, JsonValue>              callbacksDefault = (json) -> { throw new DefaultCaseNotSetException(); };

    private <B> Function1<R, JsonValue> getCaller(Function1<B, JsonValue> bodyDeserializer, Function1<R, Message<B>> callback) {
        return json -> {

            var jsonO = json.asJsonObject();
            var req = new Message<B>();
            for (var t : I(tuple(MessageSerializer.Id.ID, method((String i) -> {
                req.id = UUID.fromString(SerdesDefaults.stringFromJson.apply(jsonO.get(i)));
            })), tuple(MessageSerializer.Id.BODY, method((String i) -> {
                req.body = bodyDeserializer.apply(jsonO.get(i));
            })))) {
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

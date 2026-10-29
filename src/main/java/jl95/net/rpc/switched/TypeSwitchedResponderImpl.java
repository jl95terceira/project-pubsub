package jl95.net.rpc.switched;

import static jl95.lang.SuperPowers.tuple;

import java.util.HashMap;
import java.util.Map;

import javax.json.JsonValue;

import jl95.lang.variadic.*;
import jl95.net.io.*;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.rpc.BytesIOSRResponder;
import jl95.net.rpc.Responder;
import jl95.net.rpc.util.TypedPayload;
import jl95.net.rpc.util.serdes.TypedPayloadJsonSerdes;
import jl95.util.UVoidFuture;

public class TypeSwitchedResponderImpl implements TypeSwitchedResponder<JsonValue, JsonValue> {

    public static class DefaultNotSetException extends RuntimeException {}

    public static TypeSwitchedResponderImpl fromSimpleRpc(Responder<byte[], byte[]> responder) {
        return new TypeSwitchedResponderImpl(responder
                .adapted(SerdesDefaults.jsonFromBytes, SerdesDefaults.jsonToBytes)
                .adapted(
            TypedPayloadJsonSerdes::fromJson,
            TypedPayloadJsonSerdes::toJson
        ));
    }
    public static TypeSwitchedResponderImpl fromSr       (Sender<byte[]> sender, Receiver<byte[]> receiver) {
        return fromSimpleRpc(BytesIOSRResponder.of(receiver, sender));
    }
    public static TypeSwitchedResponderImpl fromIo       (IOStreamSupplier ios) {
        return fromSr(BytesOStreamSender.of(ios.getOutputStream()), BytesIStreamReceiver.of(ios.getInputStream()));
    }

    private final Responder<TypedPayload, TypedPayload>
        responder;
    private final Map<String, Function1<Tuple2<TypedPayload, Boolean>, TypedPayload>>
        callbacksCases   = new HashMap<>();
    private       Function1<Tuple2<TypedPayload, Boolean>, TypedPayload>
        callbacksDefault = payload -> { throw new DefaultNotSetException(); };

    private TypeSwitchedResponderImpl(Responder<TypedPayload, TypedPayload> responder) {this.responder = responder;}

    private Function1<Tuple2<TypedPayload, Boolean>, TypedPayload> makeCallback(Function1<Tuple2<JsonValue, Boolean>, JsonValue> responseFunction) {
        return tp -> {
            var r = responseFunction.apply(tp.payload);
            return tuple(new TypedPayload(tp.typeAlias, r.a1), r.a2);
        };
    }

    @Override
    public final void            addCaseWhile(String typeAlias,
                                              Function1<Tuple2<JsonValue, Boolean>, JsonValue> responseFunction) {
        callbacksCases.put(typeAlias, makeCallback(responseFunction));
    }
    @Override
    public final void            setDefaultCase(Function1<Tuple2<JsonValue, Boolean>, JsonValue> responseFunction) {
        callbacksDefault = makeCallback(responseFunction);
    }
    @Override
    public final void            removeCase(String typeAlias) {

        callbacksCases.remove(typeAlias);
    }
    @Override
    public final UVoidFuture start     () {

        new Thread(() -> responder.respondWhile(tp -> {
            var callback = callbacksCases.getOrDefault(tp.typeAlias, callbacksDefault);
            return callback.apply(tp);
        })).start();
        return responder.waitRespondingStarted();
    }
    @Override
    public final UVoidFuture   stop      () {

        return responder.stop();
    }
    @Override
    public final Boolean         isRunning () {return responder.isRunning();}
    @Override
    public final Responder<TypedPayload, TypedPayload> getBaseResponder() {return responder;}
}


package jl95.net.rpc.switched;

import java.util.HashMap;
import java.util.Map;

import jl95.lang.Awaitable;
import jl95.lang.variadic.*;
import jl95.net.rpc.Responder;
import jl95.net.rpc.ResponderIf;
import jl95.net.rpc.util.TypedPayload;
import jl95.net.rpc.util.serdes.TypedPayloadJsonSerdes;
import jl95.net.io.Ios;
import jl95.net.io.SenderReceiverIf;
import jl95.net.rpc.collections.ResponderAdaptersCollection;

public class TypeSwitchedResponder implements TypeSwitchedResponderIf<byte[], byte[]> {

    public static class DefaultNotSetException extends RuntimeException {}

    public static TypeSwitchedResponder fromSimpleRpc(ResponderIf<byte[], byte[]> responder) {
        return new TypeSwitchedResponder(ResponderAdaptersCollection.asJsonPostGetResponder(responder).adapted(
            TypedPayloadJsonSerdes::fromJson,
            TypedPayloadJsonSerdes::toJson
        ));
    }
    public static TypeSwitchedResponder fromSr       (SenderReceiverIf<byte[], byte[]> sr) {
        return fromSimpleRpc(Responder.fromSr(sr));
    }
    public static TypeSwitchedResponder fromIo       (Ios ios) {
        return fromSr(SenderReceiverIf.fromIo(ios));
    }

    private final ResponderIf<TypedPayload, TypedPayload>
        responder;
    private final Map<String, Function1<TypedPayload, TypedPayload>>
        callbacksCases   = new HashMap<>();
    private final Function1<TypedPayload, TypedPayload>
        callbacksDefault = payload -> { throw new DefaultNotSetException(); };

    private TypeSwitchedResponder(ResponderIf<TypedPayload, TypedPayload> responder) {this.responder = responder;}

    @Override
    public final void            addCase   (String typeAlias,
                                            Function1<byte[], byte[]> responseFunction) {

        callbacksCases.put(typeAlias, tp -> new TypedPayload(tp.typeAlias, responseFunction.apply(tp.payload)));
    }
    @Override
    public final void            removeCase(String typeAlias) {

        callbacksCases.remove(typeAlias);
    }
    @Override
    public final Awaitable<Void> start     () {

        return responder.respond(tp -> callbacksCases.getOrDefault(tp.typeAlias, callbacksDefault).apply(tp));
    }
    @Override
    public final Awaitable<Void> stop      () {

        return responder.stop();
    }
    @Override
    public final Boolean         isRunning () {return responder.isRunning();}
    @Override
    public final ResponderIf<TypedPayload, TypedPayload> getBaseResponder() {return responder;}
}


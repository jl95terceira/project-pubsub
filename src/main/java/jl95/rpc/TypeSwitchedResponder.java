package jl95.rpc;

import static jl95.lang.SuperPowers.*;

import java.util.HashMap;
import java.util.Map;

import jl95.lang.Awaitable;
import jl95.lang.variadic.*;
import jl95.net.Io;
import jl95.rpc.util.TypedPayload;
import jl95.rpc.util.serdes.TypedPayloadJsonSerdes;

public abstract class TypeSwitchedResponder<ABase, RBase> {

    public static class DefaultNotSetException extends RuntimeException {}

    private final Responder<TypedPayload, TypedPayload>
        responder;
    private final Map<String, Function1<TypedPayload, TypedPayload>>
        callbacksCases   = new HashMap<>();
    private final Function1<TypedPayload, TypedPayload>
        callbacksDefault = payload -> { throw new DefaultNotSetException(); };

    private TypeSwitchedResponder(Responder<TypedPayload, TypedPayload> responder) {this.responder = responder;}

    protected abstract ABase  fromBytes(byte[] requestSerial);
    protected abstract byte[] toBytes  (RBase  responseBase);

    public TypeSwitchedResponder(Io io) {
        this(RespondersCollection.getJsonResponser(io).adapted(
            TypedPayloadJsonSerdes::fromJson,
            TypedPayloadJsonSerdes::toJson
        ));
    }

    public final <A, R> void            addCase   (String                  typeAlias,
                                                   Function1<R, A>         responseFunction,
                                                   Function1<A, ABase>     requestReader,
                                                   Function1<RBase, R>     responseWriter) {

        callbacksCases.put(typeAlias, tp -> {
            return new TypedPayload(tp.typeAlias, toBytes(responseWriter.apply(responseFunction.apply(requestReader.apply(fromBytes(tp.payload))))));
        });
    }
    public final        void            addCase   (String                  typeAlias,
                                                   Function1<RBase, ABase> responseFunction) {
        addCase(typeAlias, responseFunction, self::apply, self::apply);
    }
    public final        void            removeCase(String                  typeAlias) {

        callbacksCases.remove(typeAlias);
    }
    public final        Awaitable<Void> start     () {

        return responder.start(tp -> callbacksCases.getOrDefault(tp.typeAlias, callbacksDefault).apply(tp));
    }
    public final        Awaitable<Void> stop      () {

        return responder.stop();
    }
    public final        Boolean         isRunning () {return responder.isRunning();}
    public final Responder<TypedPayload, TypedPayload> getBaseResponder() {return responder;}
    public final <As2, Rs2> TypeSwitchedResponder<As2, Rs2> adapted(Function1<As2, ABase> requestAdapter,
                                                                    Function1<RBase, Rs2> responseAdapter) {
        return new TypeSwitchedResponder<>(responder) {

            @Override protected As2 fromBytes(byte[] requestSerial) {
                return requestAdapter.apply(TypeSwitchedResponder.this.fromBytes(requestSerial));
            }
            @Override protected byte[] toBytes(Rs2 responseBase) {
                return TypeSwitchedResponder.this.toBytes(responseAdapter.apply(responseBase));
            }
        };
    }
}


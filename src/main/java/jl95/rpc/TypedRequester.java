package jl95.rpc;

import static jl95.lang.SuperPowers.*;

import jl95.lang.variadic.*;
import jl95.net.Io;
import jl95.rpc.util.TypedPayload;
import jl95.rpc.util.serdes.TypedPayloadJsonSerdes;

public abstract class TypedRequester<ABase, RBase> {

    private final Requester<TypedPayload, TypedPayload>
        requester;

    private TypedRequester(Requester<TypedPayload, TypedPayload> requester) {this.requester = requester;}

    protected abstract byte[] toBytes  (ABase  requestBase);
    protected abstract RBase  fromBytes(byte[] responseSerial);

    public  TypedRequester(Io io) {

        this(RequestersCollection.getJsonRequester(io).adapted(
            TypedPayloadJsonSerdes::toJson,
            TypedPayloadJsonSerdes::fromJson
        ));
    }

    public final <A, R> Requester<A, R>         getFunction(String              typeAlias,
                                                            Function1<ABase, A> requestWriter,
                                                            Function1<R, RBase> responseReader) {

        return requester.adapted(a -> new TypedPayload(typeAlias, toBytes(requestWriter.apply(a))),
                                 r -> responseReader.apply(fromBytes(r.payload)));
    }
    public final        Requester<ABase, RBase> getFunction(String              typeAlias) {
        return getFunction(typeAlias, self::apply, self::apply);
    }

    public final <ABase2, RBase2> TypedRequester<ABase2, RBase2> adapted(Function1<ABase, ABase2> argAdapter,
                                                                         Function1<RBase2, RBase> reAdapter) {
        return new TypedRequester<>(requester) {

            @Override protected byte[] toBytes(ABase2 requestBase) {
                return TypedRequester.this.toBytes(argAdapter.apply(requestBase));
            }
            @Override protected RBase2 fromBytes(byte[] responseSerial) {
                return reAdapter.apply(TypedRequester.this.fromBytes(responseSerial));
            }
        };
    }
}

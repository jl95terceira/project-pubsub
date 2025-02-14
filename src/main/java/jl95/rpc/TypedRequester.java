package jl95.rpc;

import static jl95.lang.SuperPowers.*;

import jl95.lang.variadic.*;
import jl95.net.Ios;
import jl95.net.Receiver;
import jl95.net.ReceiverIf;
import jl95.net.Sender;
import jl95.net.SenderIf;
import jl95.rpc.util.TypedPayload;
import jl95.rpc.util.serdes.TypedPayloadJsonSerdes;

public class TypedRequester implements TypedRequesterIf<byte[], byte[]> {

    public static TypedRequester fromSimpleRpc(RequesterIf<byte[], byte[]> requester) {

        return new TypedRequester(RequesterAdaptersCollection.getJsonRequester(requester).adapted(
            TypedPayloadJsonSerdes::toJson,
            TypedPayloadJsonSerdes::fromJson
        ));
    }
    public static TypedRequester fromSr       (SenderIf  <byte[]> sender,
                                               ReceiverIf<byte[]> receiver) {
        return fromSimpleRpc(Requester.fromSr(sender, receiver));
    }
    public static TypedRequester fromIo       (Ios io) {

        return fromSr(Sender.of(io.getOutputStream()), Receiver.of(io.getInputStream()));
    }

    private final RequesterIf<TypedPayload, TypedPayload>
        requester;

    private TypedRequester(RequesterIf<TypedPayload, TypedPayload> requester) {this.requester = requester;}

    @Override
    public final RequesterIf<byte[], byte[]> getFunction(String typeAlias) {

        return requester.adapted(a -> new TypedPayload(typeAlias, a),
                                 r -> r.payload);
    }
}

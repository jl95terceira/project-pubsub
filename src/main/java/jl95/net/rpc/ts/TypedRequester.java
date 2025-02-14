package jl95.net.rpc.ts;

import jl95.net.rpc.Requester;
import jl95.net.rpc.RequesterAdaptersCollection;
import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.util.TypedPayload;
import jl95.net.rpc.util.serdes.TypedPayloadJsonSerdes;
import jl95.net.sr.Ios;
import jl95.net.sr.SrIf;

public class TypedRequester implements TypedRequesterIf<byte[], byte[]> {

    public static TypedRequester fromSimpleRpc(RequesterIf<byte[], byte[]> requester) {

        return new TypedRequester(RequesterAdaptersCollection.asJsonRequester(requester).adapted(
            TypedPayloadJsonSerdes::toJson,
            TypedPayloadJsonSerdes::fromJson
        ));
    }
    public static TypedRequester fromSr       (SrIf<byte[], byte[]> sr) {
        return fromSimpleRpc(Requester.fromSr(sr));
    }
    public static TypedRequester fromIo       (Ios ios) {

        return fromSr(SrIf.of(ios));
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

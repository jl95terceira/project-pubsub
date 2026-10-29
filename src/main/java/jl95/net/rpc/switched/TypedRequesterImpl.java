package jl95.net.rpc.switched;

import javax.json.JsonValue;

import jl95.net.io.*;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.rpc.BytesIOSRRequester;
import jl95.net.rpc.IOSRRequester;
import jl95.net.rpc.Requester;
import jl95.net.rpc.util.TypedPayload;
import jl95.net.rpc.util.serdes.TypedPayloadJsonSerdes;

public class TypedRequesterImpl implements TypedRequester<JsonValue, JsonValue> {

    public static TypedRequesterImpl fromSimpleRpc(Requester<byte[], byte[]> requester) {

        return new TypedRequesterImpl(requester
                .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                .adapted(
            TypedPayloadJsonSerdes::toJson,
            TypedPayloadJsonSerdes::fromJson
        ));
    }
    public static TypedRequesterImpl fromSr       (Sender<byte[]> sender, Receiver<byte[]> receiver) {
        return fromSimpleRpc(BytesIOSRRequester.of(sender, receiver));
    }
    public static TypedRequesterImpl fromIo       (IOStreamSupplier ios) {

        return fromSr(BytesOStreamSender.of(ios.getOutputStream()), BytesIStreamReceiver.of(ios.getInputStream()));
    }
    public static TypedRequesterImpl fromManagedIo(ManagedIOStream ios) {

        return fromSr(BytesOStreamSender.of(ios.output()), BytesIStreamReceiver.of(ios.input()));
    }

    private final Requester<TypedPayload, TypedPayload>
        requester;

    private TypedRequesterImpl(Requester<TypedPayload, TypedPayload> requester) {this.requester = requester;}

    @Override
    public final Requester<JsonValue, JsonValue> getFunction(String typeAlias) {

        return requester.adapted(a -> new TypedPayload(typeAlias, a),
                                 r -> r.payload);
    }
}

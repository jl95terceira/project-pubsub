package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.IosSupplier;
import jl95.rpc.util.SerdesDefaults;

public class RequestersCollection {

    public static Requester<byte[],    byte[]>    getBytesRequester (IosSupplier io) {

        return new Requester<>(io) {

            @Override protected byte[] writeRequest(byte[] object) {
                return object;
            }
            @Override protected byte[] readResponse(byte[] serial) { return serial; }
        };
    }
    public static Requester<String,    String>    getStringRequester(IosSupplier io) {

        return getBytesRequester(io).adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
    public static Requester<JsonValue, JsonValue> getJsonRequester  (IosSupplier io) {

        return getStringRequester(io).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
}

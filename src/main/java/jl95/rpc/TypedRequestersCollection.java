package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.IosSupplier;
import jl95.rpc.util.SerdesDefaults;

public class TypedRequestersCollection {

    private TypedRequestersCollection() {}

    public static TypedRequester<byte[],    byte[]>    getBytesRequester (IosSupplier io) {
        return new TypedRequester<>(io) {

            @Override protected byte[] toBytes(byte[] requestBase) {
                return requestBase;
            }
            @Override protected byte[] fromBytes(byte[] responseSerial) {
                return responseSerial;
            }
        };
    }
    public static TypedRequester<String,    String>    getStringRequester(IosSupplier io) {
        return getBytesRequester(io).adapted(SerdesDefaults.stringToBytes, SerdesDefaults.stringFromBytes);
    }
    public static TypedRequester<JsonValue, JsonValue> getJsonRequester  (IosSupplier io) {
        return getStringRequester(io).adapted(SerdesDefaults.jsonToString, SerdesDefaults.jsonFromString);
    }
}


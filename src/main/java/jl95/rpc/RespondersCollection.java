package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.IosSupplier;
import jl95.rpc.util.SerdesDefaults;

public class RespondersCollection {

    public static Responder<byte[],      byte[]>    getBytesResponder (IosSupplier io) {

        return new Responder<>(io) {

            @Override protected byte[] readRequest  (byte[] serial) { return serial; }
            @Override protected byte[] writeResponse(byte[] object) {
                        return object;
                    }
        };
    }
    public static Responder<String,    String> getStringResponser(IosSupplier io) {

        return getBytesResponder(io).adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }
    public static Responder<JsonValue, JsonValue> getJsonResponser  (IosSupplier io) {

        return getStringResponser(io).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
}

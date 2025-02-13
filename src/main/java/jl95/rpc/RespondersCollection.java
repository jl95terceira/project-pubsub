package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Ios;
import jl95.rpc.util.SerdesDefaults;

public class RespondersCollection {

    public static Responder<byte[],      byte[]>    getBytesResponder (Ios io) {

        return new Responder<>(io) {

            @Override protected byte[] readRequest  (byte[] serial) { return serial; }
            @Override protected byte[] writeResponse(byte[] object) {
                        return object;
                    }
        };
    }
    public static Responder<String,    String> getStringResponser(Ios io) {

        return getBytesResponder(io).adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }
    public static Responder<JsonValue, JsonValue> getJsonResponser  (Ios io) {

        return getStringResponser(io).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
}

package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Io;
import jl95.rpc.util.SerdesDefaults;

public class RespondersCollection {

    public static Responder<byte[],      byte[]>    getBytesResponder (Io io) {

        return new Responder<>(io) {

            @Override protected byte[] readRequest  (byte[] serial) { return serial; }
            @Override protected byte[] writeResponse(byte[] object) {
                        return object;
                    }
        };
    }
    public static ResponderIf<String,    String>    getStringResponser(Io io) {

        return getBytesResponder(io).adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }
    public static ResponderIf<JsonValue, JsonValue> getJsonResponser  (Io io) {

        return getStringResponser(io).adapted(
            SerdesDefaults.jsonFromString,
            SerdesDefaults.jsonToString
        );
    }
}

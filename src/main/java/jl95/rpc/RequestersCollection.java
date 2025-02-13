package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Ios;
import jl95.rpc.util.SerdesDefaults;

public class RequestersCollection {

    public static Requester<byte[],    byte[]>    getBytesRequester (Ios io) {

        return new Requester<>(io) {

            @Override protected byte[] writeRequest(byte[] object) {
                return object;
            }
            @Override protected byte[] readResponse(byte[] serial) { return serial; }
        };
    }
    public static Requester<String,    String>    getStringRequester(Ios io) {

        return getBytesRequester(io).adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
    public static Requester<JsonValue, JsonValue> getJsonRequester  (Ios io) {

        return getStringRequester(io).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
}

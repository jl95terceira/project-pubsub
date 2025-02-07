package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Io;
import jl95.rpc.util.SerdesDefaults;

public class RequestersCollection {

    public static RequesterIf<byte[],    byte[]>    getBytesRequester (Io io) {

        return new Requester<>(io) {

            @Override protected byte[] writeRequest(byte[] object) {
                return object;
            }
            @Override protected byte[] readResponse(byte[] serial) { return serial; }
        };
    }
    public static RequesterIf<String,    String>    getStringRequester(Io io) {

        return getBytesRequester(io).adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
    public static RequesterIf<JsonValue, JsonValue> getJsonRequester  (Io io) {

        return getStringRequester(io).adapted(
            SerdesDefaults.jsonToString,
            SerdesDefaults.jsonFromString
        );
    }
}

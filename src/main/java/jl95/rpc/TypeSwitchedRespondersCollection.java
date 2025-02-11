package jl95.rpc;

import javax.json.JsonValue;

import jl95.net.Io;
import jl95.rpc.util.SerdesDefaults;

public class TypeSwitchedRespondersCollection {

    private TypeSwitchedRespondersCollection() {}

    public static TypeSwitchedResponder<byte[],    byte[]>    getBytesResponder (Io io) {
        return new TypeSwitchedResponder<>(io) {

            @Override protected byte[] fromBytes(byte[] requestSerial) {
                return requestSerial;
            }
            @Override protected byte[] toBytes(byte[] responseBase) {
                return responseBase;
            }
        };
    }
    public static TypeSwitchedResponder<String,    String>    getStringResponder(Io io) {
        return getBytesResponder(io).adapted(SerdesDefaults.stringFromBytes, SerdesDefaults.stringToBytes);
    }
    public static TypeSwitchedResponder<JsonValue, JsonValue> getJsonResponder  (Io io) {
        return getStringResponder(io).adapted(SerdesDefaults.jsonFromString, SerdesDefaults.jsonToString);
    }
}

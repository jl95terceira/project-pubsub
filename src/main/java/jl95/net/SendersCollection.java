package jl95.net;

import java.io.OutputStream;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class SendersCollection {

    private SendersCollection() {}

    public static Sender<byte[]>    getBytesSender (OutputStream os) {

        return new Sender<>(os) {
                @Override protected byte[] toBytes(byte[] bytes)  { return bytes; }
        };
    }
    public static Sender<String>    getStringSender(OutputStream os) {

        return getBytesSender(os).adapted(SerdesDefaults.stringToBytes);
    }
    public static Sender<JsonValue> getJsonSender  (OutputStream os) {

        return getStringSender(os).adapted(SerdesDefaults.jsonToString);
    }
}

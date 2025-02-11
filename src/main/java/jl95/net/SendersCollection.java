package jl95.net;

import java.io.OutputStream;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class SendersCollection {

    private SendersCollection() {}

    public static Sender<byte[]>    getBytesSender (OsSupplier os) {

        return new Sender<>(os) {
                @Override protected byte[] toBytes(byte[] bytes)  { return bytes; }
        };
    }
    public static Sender<String>    getStringSender(OsSupplier os) {

        return getBytesSender(os).adapted(SerdesDefaults.stringToBytes);
    }
    public static Sender<JsonValue> getJsonSender  (OsSupplier os) {

        return getStringSender(os).adapted(SerdesDefaults.jsonToString);
    }
}

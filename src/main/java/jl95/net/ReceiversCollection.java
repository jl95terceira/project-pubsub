package jl95.net;

import java.io.InputStream;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class ReceiversCollection {

    private ReceiversCollection() {}

    public static Receiver<byte[]>    getBytesReceiver (InputStream is) {

        return new Receiver<>(is) {
                @Override protected byte[] fromBytes(byte[] bytes)  { return bytes; }
        };
    }
    public static Receiver<String>    getStringReceiver(InputStream is) {

        return getBytesReceiver(is).adapted(SerdesDefaults.stringFromBytes);
    }
    public static Receiver<JsonValue> getJsonReceiver  (InputStream is) {

        return getStringReceiver(is).adapted(SerdesDefaults.jsonFromString);
    }
}

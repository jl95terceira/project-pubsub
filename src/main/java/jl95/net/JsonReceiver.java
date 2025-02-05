package jl95.net;

import java.io.InputStream;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class JsonReceiver extends Receiver<JsonValue> {

    public JsonReceiver(InputStream in) {
        super(in);
    }

    @Override protected JsonValue fromBytes(byte[] incoming)  { return SerdesDefaults.jsonFromBytes.call(incoming); }
}

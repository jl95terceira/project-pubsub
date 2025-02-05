package jl95.net;

import java.io.OutputStream;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class JsonSender extends Sender<JsonValue> {

    public JsonSender(OutputStream out) { super(out); }

    @Override protected byte[] toBytes  (JsonValue outgoing) { return SerdesDefaults.jsonToBytes.call(outgoing); }
}

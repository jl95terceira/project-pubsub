package jl95.net.io.collections;

import javax.json.JsonValue;

import jl95.net.io.Sender;
import jl95.net.io.SenderIf;
import jl95.net.pubsub.util.SerdesDefaults;

public class SenderAdaptersCollections {

    private SenderAdaptersCollections() {}

    public static SenderIf<String>    asStringSender(SenderIf<byte[]> sender) {

        return sender.adaptedSender(SerdesDefaults.stringToBytes);
    }
    public static SenderIf<JsonValue> asJsonSender  (SenderIf<byte[]> sender) {

        return asStringSender(sender).adaptedSender(SerdesDefaults.jsonToString);
    }
}

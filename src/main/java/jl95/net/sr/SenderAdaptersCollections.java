package jl95.net.sr;

import javax.json.JsonValue;

import jl95.net.pubsub.util.SerdesDefaults;

public class SenderAdaptersCollections {

    private SenderAdaptersCollections() {}

    public static SenderIf<String> asStringSender(Sender sender) {

        return sender.adaptedSender(SerdesDefaults.stringToBytes);
    }
    public static SenderIf<JsonValue> asJsonSender  (Sender sender) {

        return asStringSender(sender).adaptedSender(SerdesDefaults.jsonToString);
    }
}

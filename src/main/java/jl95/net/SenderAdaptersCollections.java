package jl95.net;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class SenderAdaptersCollections {

    private SenderAdaptersCollections() {}

    public static SenderIf<String>    getStringSender(Sender sender) {

        return sender.adaptedSender(SerdesDefaults.stringToBytes);
    }
    public static SenderIf<JsonValue> getJsonSender  (Sender sender) {

        return getStringSender(sender).adaptedSender(SerdesDefaults.jsonToString);
    }
}

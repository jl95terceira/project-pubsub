package jl95.net;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class ReceiverAdaptersCollection {

    private ReceiverAdaptersCollection() {}

    public static ReceiverIf<String>    getStringReceiver(Receiver receiver) {

        return receiver.adaptedReceiver(SerdesDefaults.stringFromBytes);
    }
    public static ReceiverIf<JsonValue> getJsonReceiver  (Receiver receiver) {

        return getStringReceiver(receiver).adaptedReceiver(SerdesDefaults.jsonFromString);
    }
}

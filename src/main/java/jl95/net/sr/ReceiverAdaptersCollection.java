package jl95.net.sr;

import javax.json.JsonValue;

import jl95.net.pubsub.util.SerdesDefaults;

public class ReceiverAdaptersCollection {

    private ReceiverAdaptersCollection() {}

    public static ReceiverIf<String> asStringReceiver(Receiver receiver) {

        return receiver.adaptedReceiver(SerdesDefaults.stringFromBytes);
    }
    public static ReceiverIf<JsonValue> asJsonReceiver  (Receiver receiver) {

        return asStringReceiver(receiver).adaptedReceiver(SerdesDefaults.jsonFromString);
    }
}

package jl95.net.io.collections;

import javax.json.JsonValue;

import jl95.net.io.Receiver;
import jl95.net.io.ReceiverIf;
import jl95.net.pubsub.util.SerdesDefaults;

public class ReceiverAdaptersCollection {

    private ReceiverAdaptersCollection() {}

    public static ReceiverIf<String>    asStringReceiver(ReceiverIf<byte[]> receiver) {

        return receiver.adaptedReceiver(SerdesDefaults.stringFromBytes);
    }
    public static ReceiverIf<JsonValue> asJsonReceiver  (ReceiverIf<byte[]> receiver) {

        return asStringReceiver(receiver).adaptedReceiver(SerdesDefaults.jsonFromString);
    }
}

package jl95.net.pubsub.collections;

import javax.json.JsonValue;

import jl95.net.pubsub.Consumer;
import jl95.net.pubsub.Producer;
import jl95.net.pubsub.util.SerdesDefaults;

public class MemberAdaptersCollection {

    public static Producer<String> getStringProducer(Producer<JsonValue> client) {
        return client.adaptedProducer(SerdesDefaults.stringToJson);
    }
    public static Consumer<String> getStringConsumer(Consumer<JsonValue> client) {
        return client.adaptedConsumer(SerdesDefaults.stringFromJson);
    }
    public static Producer<byte[]> getBytesProducer  (Producer<JsonValue> client) {
        return getStringProducer(client).adaptedProducer(SerdesDefaults.bytesToString);
    }
    public static Consumer<byte[]> getBytesConsumer  (Consumer<JsonValue> client) {
        return getStringConsumer(client).adaptedConsumer(SerdesDefaults.bytesFromString);
    }
}

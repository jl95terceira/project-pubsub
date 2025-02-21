package jl95.net.pubsub.collections;

import javax.json.JsonValue;

import jl95.net.pubsub.ConsumerIf;
import jl95.net.pubsub.ProducerIf;
import jl95.net.pubsub.util.SerdesDefaults;

public class MemberAdaptersCollection {

    public static ProducerIf<String> getStringProducer(ProducerIf<JsonValue> client) {
        return client.adaptedProducer(SerdesDefaults.stringToJson);
    }
    public static ConsumerIf<String> getStringConsumer(ConsumerIf<JsonValue> client) {
        return client.adaptedConsumer(SerdesDefaults.stringFromJson);
    }
    public static ProducerIf<byte[]> getBytesProducer  (ProducerIf<JsonValue> client) {
        return getStringProducer(client).adaptedProducer(SerdesDefaults.bytesToString);
    }
    public static ConsumerIf<byte[]> getBytesConsumer  (ConsumerIf<JsonValue> client) {
        return getStringConsumer(client).adaptedConsumer(SerdesDefaults.bytesFromString);
    }
}

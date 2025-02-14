package jl95.pubsub;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class ClientAdaptersCollection {

    public static ProducerIf<String>    getStringProducer(ProducerIf<byte[]> client) {
        return client.adaptedProducer(SerdesDefaults.stringToBytes);
    }
    public static ConsumerIf<String>    getStringConsumer(ConsumerIf<byte[]> client) {
        return client.adaptedConsumer(SerdesDefaults.stringFromBytes);
    }
    public static ProducerIf<JsonValue> getJsonProducer  (ProducerIf<byte[]> client) {
        return getStringProducer(client).adaptedProducer(SerdesDefaults.jsonToString);
    }
    public static ConsumerIf<JsonValue> getJsonConsumer  (ConsumerIf<byte[]> client) {
        return getStringConsumer(client).adaptedConsumer(SerdesDefaults.jsonFromString);
    }
}

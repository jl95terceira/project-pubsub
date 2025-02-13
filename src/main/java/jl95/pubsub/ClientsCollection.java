package jl95.pubsub;

import java.net.InetSocketAddress;

import javax.json.JsonValue;

import jl95.pubsub.util.SerdesDefaults;

public class ClientsCollection {

    public static Client<byte[],    byte[]>    getBytesClient (InetSocketAddress serverAddress) {
        return new Client<>(serverAddress) {
            @Override
            protected byte[] toBytes(byte[] preSerial) {
                return preSerial;
            }

            @Override
            protected byte[] fromBytes(byte[] serial) {
                return serial;
            }
        };
    }
    public static Client<String,    String>    getStringClient(InetSocketAddress serverAddress) {
        return getBytesClient(serverAddress).adapted(SerdesDefaults.stringToBytes, SerdesDefaults.stringFromBytes);
    }
    public static Client<JsonValue, JsonValue> getJsonClient  (InetSocketAddress serverAddress) {
        return getStringClient(serverAddress).adapted(SerdesDefaults.jsonToString, SerdesDefaults.jsonFromString);
    }
}

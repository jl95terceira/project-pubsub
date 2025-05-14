package jl95.net.pubsub.util;

import java.util.Base64;
import java.util.List;
import java.util.UUID;

import javax.json.JsonValue;

import jl95.lang.variadic.*;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.serdes.*;

public class SerdesDefaults {

    public static final Function1<byte[], String>
                                    stringToBytes           = StringUTF8ToBytes.get();
    public static final Function1<String, byte[]>
                                    stringFromBytes         = StringUTF8FromBytes.get();
    public static final Function1<JsonValue, String>
                                    stringToJson            = StringToJson  .get();
    public static final Function1<String, JsonValue>
                                    stringFromJson          = StringFromJson.get();
    public static final Function1<JsonValue, Boolean>
                                    boolToJson              = BooleanToJson  .get();
    public static final Function1<Boolean, JsonValue>
                                    boolFromJson            = BooleanFromJson.get();
    public static final Function1<String, JsonValue>
                                    jsonToString            = JsonToString.get();
    public static final Function1<JsonValue, String>
                                    jsonFromString          = JsonFromString.get();
    public static final Function1<byte[], JsonValue>
                                    jsonToBytes             = json   -> stringToBytes.apply
                                                                       (jsonToString .apply(json));
    public static final Function1<JsonValue, byte[]>
                                    jsonFromBytes           = serial -> jsonFromString .apply
                                                                       (stringFromBytes.apply(serial));
    public static final Function1<String, byte[]>
                                    bytesToString           = Base64.getEncoder()::encodeToString;
    public static final Function1<byte[], String>
                                    bytesFromString         = Base64.getDecoder()::decode;
    public static final Function1<JsonValue, Iterable<String>>
                                    listOfStringToJson      = ListOfStringToJson.get();
    public static final Function1<List<String>, JsonValue>
                                    listOfStringFromJson    = ListOfStringFromJson.get();
}

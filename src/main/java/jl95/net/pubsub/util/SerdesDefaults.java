package jl95.net.pubsub.util;

import java.util.Base64;
import java.util.List;

import javax.json.JsonValue;

import jl95.lang.variadic.*;
import jl95.net.pubsub.Message;
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
    public static final Function1<String, JsonValue>
                                    jsonToString            = JsonToString.get();
    public static final Function1<JsonValue, String>
                                    jsonFromString          = JsonFromString.get();
    public static final Function1<byte[], JsonValue>
                                    jsonToBytes             = json   -> stringToBytes.call
                                                                       (jsonToString .call(json));
    public static final Function1<JsonValue, byte[]>
                                    jsonFromBytes           = serial -> jsonFromString .call
                                                                       (stringFromBytes.call(serial));
    public static final Function1<JsonValue, Message<Publication>>
                                    pubMsgToJson            = MessageSerializer.get(MessageType.PUBLISH                 .serial, PublicationJsonSerdes::toJson);
    public static final Function1<JsonValue, Message<Close>>
                                    closeReqToJson          = MessageSerializer.get(MessageType.REQ_CLOSE               .serial, CloseJsonSerdes::toJson);
    public static final Function1<JsonValue, Message<SubscriptionByList>>
                                    subListReqToJson        = MessageSerializer.get(MessageType.REQ_SUBSCRIPTION_BY_LIST.serial, SubscriptionByListJsonSerdes::toJson);
    public static final Function1<JsonValue, Message<SubscriptionByRegex>>
                                    subRegexReqToJson       = MessageSerializer.get(MessageType.REQ_SUBSCRIPTION_BY_LIST.serial, SubscriptionByRegexJsonSerdes::toJson);
    public static final Function1<JsonValue, Message<SubscriptionToAll>>
                                    subAllReqToJson         = MessageSerializer.get(MessageType.REQ_SUBSCRIPTION_TO_ALL .serial, SubscriptionToAllJsonSerdes::toJson);
    public static final Function1<JsonValue, Message<SubscriptionToNone>>
                                    subNoneReqToJson        = MessageSerializer.get(MessageType.REQ_SUBSCRIPTION_TO_NONE.serial, SubscriptionToNoneJsonSerdes::toJson);
    public static final Function1<String, byte[]>
                                    bytesToString           = Base64.getEncoder()::encodeToString;
    public static final Function1<byte[], String>
                                    bytesFromString         = Base64.getDecoder()::decode;
    public static final Function1<JsonValue, Iterable<String>>
                                    listOfStringToJson      = ListOfStringToJson.get();
    public static final Function1<List<String>, JsonValue>
                                    listOfStringFromJson    = ListOfStringFromJson.get();
}

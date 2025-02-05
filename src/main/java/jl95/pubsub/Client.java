package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import javax.json.JsonValue;

import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.net.JsonReceiver;
import jl95.net.JsonSender;
import jl95.net.Receiver;
import jl95.net.Sender;
import jl95.net.util.ReceiverBySocket;
import jl95.net.util.SenderBySocket;
import jl95.pubsub.protocol.Message;
import jl95.pubsub.protocol.Publication;
import jl95.pubsub.protocol.requests.Close;
import jl95.pubsub.protocol.requests.SubscriptionByList;
import jl95.pubsub.protocol.requests.SubscriptionByRegex;
import jl95.pubsub.protocol.requests.SubscriptionToAll;
import jl95.pubsub.protocol.requests.SubscriptionToNone;
import jl95.pubsub.serdes.MessageSwitchingDeserializer;
import jl95.pubsub.serdes.PublicationJsonSerdes;
import jl95.pubsub.util.MessageType;
import jl95.pubsub.util.SerdesDefaults;

public class Client {

    private static Socket getConnectedSocket(InetSocketAddress serverAddr) {
        var socket = new Socket();
        uncheck(() -> socket.connect(serverAddr));
        return socket;
    }

    public interface    Options {

        class Editable implements Options {
    }
        static Options defaults() {return new Editable();}
    }

    private final Socket                                socket;
    private final Sender<Message<Publication>>          pubSender;
    private final Sender<Message<Close>>                closeSender;
    private final Sender<Message<SubscriptionByList>>   subListSender;
    private final Sender<Message<SubscriptionByRegex>>  subReSender;
    private final Sender<Message<SubscriptionToAll>>    subAllSender;
    private final Sender<Message<SubscriptionToNone>>   subNoneSender;
    private final Receiver<JsonValue>                   jsonReceiver;
    private final MessageSwitchingDeserializer<Boolean> switchDeser;
    private       Method1<Publication>                  pubCallback = (pub) -> {/* pass */};

    synchronized private <T> void sendMessage(T object, Sender<Message<T>> sender) {
        var msg = new Message<T>();
        msg.id   = UUID.randomUUID();
        msg.body = object;
        sender.send(msg);
    }

    public Client(Socket            socket,
                  Options           options) {
        this.socket = socket;
        var jsonSender = SenderBySocket.get(socket, JsonSender::new);
        this.pubSender     = jsonSender.extend(SerdesDefaults.pubMsgToJson);
        this.closeSender   = jsonSender.extend(SerdesDefaults.closeReqToJson);
        this.subListSender = jsonSender.extend(SerdesDefaults.subListReqToJson);
        this.subReSender   = jsonSender.extend(SerdesDefaults.subRegexReqToJson);
        this.subAllSender  = jsonSender.extend(SerdesDefaults.subAllReqToJson);
        this.subNoneSender = jsonSender.extend(SerdesDefaults.subNoneReqToJson);
        this.jsonReceiver  = ReceiverBySocket.get(socket, JsonReceiver::new);
        this.switchDeser   = new MessageSwitchingDeserializer<>();
        switchDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            msg -> {
                pubCallback.accept(msg.body);
                return true;
            }
        );
    }
    public Client(InetSocketAddress serverAddr,
                  Options           options) {
        this(getConnectedSocket(serverAddr), options);
    }

    synchronized public final void         produce         (Publication          pub) {

        sendMessage(pub, pubSender);
    }
    synchronized public final void         produce         (String               topicName,
                                                            byte[]               data) {

        var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = data;
        produce(pub);
    }
    synchronized public final void         consume         () {

        jsonReceiver.recvWhile(switchDeser);
    }
    synchronized public final Future<Void> consumeStop     () {

        return jsonReceiver.recvStop();
    }
    synchronized public final void         consumeStopAwait() {

        jsonReceiver.recvStopAwait();
    }
    synchronized public final Boolean      isConsuming     () {

        return jsonReceiver.isReceiving();
    }
    synchronized public final void         onConsumed      (Method1<Publication> pubCallback) {

        if (!isConsuming()) {
            consume();
        }
        this.pubCallback = pubCallback;
    }

    public final void close           () {

        sendMessage(new Close(), closeSender);
        uncheck(socket::close);
    }
    public final void subscribeByList (Set<String>      topicNames) {

        var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        sendMessage(sub, subListSender);
    }
    public final void subscribeByList (Iterable<String> topicNames) {

        subscribeByList(I.of(topicNames).toSet());
    }
    public final void subscribeByRegex(Pattern          topicPattern) {

        var sub = new SubscriptionByRegex();
        sub.topicPattern = topicPattern;
        sendMessage(sub, subReSender);
    }
    public final void subscribeByRegex(String           topicPattern) {

        subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final void subscribeToAll  () {

        sendMessage(new SubscriptionToAll(), subAllSender);
    }
    public final void subscribeToNone () {

        sendMessage(new SubscriptionToNone(), subNoneSender);
    }
}

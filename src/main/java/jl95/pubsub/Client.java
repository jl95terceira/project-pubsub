package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.net.IosSupplier;
import jl95.net.IsSupplier;
import jl95.net.OsSupplier;
import jl95.net.Receiver;
import jl95.net.ReceiversCollection;
import jl95.net.Sender;
import jl95.net.SendersCollection;
import jl95.pubsub.util.Message;
import jl95.pubsub.protocol.Publication;
import jl95.pubsub.protocol.Close;
import jl95.pubsub.protocol.SubscriptionByList;
import jl95.pubsub.protocol.SubscriptionByRegex;
import jl95.pubsub.protocol.SubscriptionToAll;
import jl95.pubsub.protocol.SubscriptionToNone;
import jl95.pubsub.util.serdes.MessageSwitchedDeserializer;
import jl95.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.pubsub.util.MessageType;
import jl95.pubsub.util.SerdesDefaults;
import jl95.rpc.util.CloseableIosSupplier;

public class Client {

    private static Socket getConnectedSocket(InetSocketAddress serverAddr) {
        var socket = new Socket();
        uncheck(() -> socket.connect(serverAddr));
        return socket;
    }

    public interface    Options {

        /* no methods yet but soon to have */

        class Editable implements Options {
    }
        static Options defaults() {return new Editable();}
    }

    private final Method0                               closer;
    private final Sender<Message<Publication>>          pubSender;
    private final Sender<Message<Close>>                closeSender;
    private final Sender<Message<SubscriptionByList>>   subListSender;
    private final Sender<Message<SubscriptionByRegex>>  subReSender;
    private final Sender<Message<SubscriptionToAll>>    subAllSender;
    private final Sender<Message<SubscriptionToNone>>   subNoneSender;
    private final Receiver<JsonValue>                   jsonReceiver;
    private final MessageSwitchedDeserializer<Boolean>  switchDeser;
    private       Method1<Publication>                  pubCallback = (pub) -> {/* pass */};

    synchronized private <T> void sendMessage(T object, Sender<Message<T>> sender) {
        var msg = new Message<T>();
        msg.id   = UUID.randomUUID();
        msg.body = object;
        sender.send(msg);
    }

    public Client(CloseableIosSupplier  iosSupplier,
                  Options               options) {
        this.closer = unchecked(iosSupplier::close);
        var jsonSender = SendersCollection.getJsonSender(iosSupplier);
        this.pubSender     = jsonSender.adapted(SerdesDefaults.pubMsgToJson);
        this.closeSender   = jsonSender.adapted(SerdesDefaults.closeReqToJson);
        this.subListSender = jsonSender.adapted(SerdesDefaults.subListReqToJson);
        this.subReSender   = jsonSender.adapted(SerdesDefaults.subRegexReqToJson);
        this.subAllSender  = jsonSender.adapted(SerdesDefaults.subAllReqToJson);
        this.subNoneSender = jsonSender.adapted(SerdesDefaults.subNoneReqToJson);
        this.jsonReceiver  = ReceiversCollection.getJsonReceiver(iosSupplier);
        this.switchDeser   = new MessageSwitchedDeserializer<>();
        switchDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            msg -> {
                pubCallback.accept(msg.body);
                return true;
            }
        );
    }
    public Client(Socket                clientSocket,
                  Options               options) {
        this(CloseableIosSupplier.of(clientSocket), options);
    }
    public Client(InetSocketAddress     serverAddr,
                  Options               options) {
        this(getConnectedSocket(serverAddr), options);
    }

    synchronized public final void            produce         (Publication          pub) {

        sendMessage(pub, pubSender);
    }
    synchronized public final void            produce         (String               topicName,
                                                               byte[]               data) {

        var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = data;
        produce(pub);
    }
    synchronized public final void            consume         () {

        jsonReceiver.recvWhile(switchDeser);
    }
    synchronized public final Awaitable<Void> consumeStop     () {

        return jsonReceiver.recvStop();
    }
    synchronized public final Boolean         isConsuming     () {

        return jsonReceiver.isReceiving();
    }
    synchronized public final void            onConsumed      (Method1<Publication>    pubCallback) {

        if (!isConsuming()) {
            consume();
        }
        this.pubCallback = pubCallback;
    }
    synchronized public final void            onConsumed      (Method2<String, byte[]> pubCallback) {

        onConsumed(pub -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }

    public final void close           () {

        sendMessage(new Close(), closeSender);
        closer.accept();
    }
    public final void subscribe       (SubscriptionByList  sub) {
        subscribeByList(sub.topicNames);
    }
    public final void subscribeByList (Set<String>         topicNames) {

        var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        sendMessage(sub, subListSender);
    }
    public final void subscribeByList (Iterable<String>    topicNames) {

        subscribeByList(I.of(topicNames).toSet());
    }
    public final void subscribe       (SubscriptionByRegex sub) {
        subscribeByRegex(sub.topicPattern);
    }
    public final void subscribeByRegex(Pattern             topicPattern) {

        var sub = new SubscriptionByRegex();
        sub.topicPattern = topicPattern;
        sendMessage(sub, subReSender);
    }
    public final void subscribeByRegex(String              topicPattern) {

        subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final void subscribe       (SubscriptionToAll   sub) {
        subscribeToAll();
    }
    public final void subscribeToAll  () {

        sendMessage(new SubscriptionToAll(), subAllSender);
    }
    public final void subscribe       (SubscriptionToNone  sub) {
        subscribeToNone();
    }
    public final void subscribeToNone () {

        sendMessage(new SubscriptionToNone(), subNoneSender);
    }
}

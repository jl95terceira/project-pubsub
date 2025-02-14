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
import jl95.net.Receiver;
import jl95.net.ReceiverIf;
import jl95.net.ReceiverAdaptersCollection;
import jl95.net.Sender;
import jl95.net.SenderIf;
import jl95.net.SenderAdaptersCollections;
import jl95.net.util.Util;
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
import jl95.net.CloseableIos;
import jl95.rpc.Requester;
import jl95.rpc.RequesterIf;
import jl95.rpc.RequesterAdaptersCollection;

public class Client implements ClientIf<byte[], byte[]> {

    private static class SenderIfs {

        private final SenderIf<JsonValue>                   jsonSender;
        private final SenderIf<Message<Publication>>        pubSender;
        private final SenderIf<Message<Close>>              closeSender;
        private final SenderIf<Message<SubscriptionByList>> subListSender;
        private final SenderIf<Message<SubscriptionByRegex>>subReSender;
        private final SenderIf<Message<SubscriptionToAll>>  subAllSender;
        private final SenderIf<Message<SubscriptionToNone>> subNoneSender;

        public SenderIfs(Sender sender) {
            this.jsonSender    = SenderAdaptersCollections.getJsonSender(sender);
            this.pubSender     = jsonSender.adaptedSender(SerdesDefaults.pubMsgToJson);
            this.closeSender   = jsonSender.adaptedSender(SerdesDefaults.closeReqToJson);
            this.subListSender = jsonSender.adaptedSender(SerdesDefaults.subListReqToJson);
            this.subReSender   = jsonSender.adaptedSender(SerdesDefaults.subRegexReqToJson);
            this.subAllSender  = jsonSender.adaptedSender(SerdesDefaults.subAllReqToJson);
            this.subNoneSender = jsonSender.adaptedSender(SerdesDefaults.subNoneReqToJson);
        }

    }

    private final CloseableIos                          ios;
    private final UUID                                  clientId = UUID.randomUUID();
    private final Method0                               closer;
    private final Sender                                sender;
    private final SenderIfs                             senderIfs;
    private final RequesterIf<String, String>           clientRegRequester;
    private final Receiver                              receiver;
    private final ReceiverIf<JsonValue>                 jsonReceiver;
    private final MessageSwitchedDeserializer<Boolean>  switchDeser;
    private       Method1<Publication>                  pubCallback = (pub) -> {/* pass */};

    private Client(CloseableIos      ios) {
        this.ios    = ios;
        this.closer = unchecked(ios::close);
        this.sender     = Sender.of(ios.getOutputStream());
        this.senderIfs  = new SenderIfs(sender);
        this.receiver   = Receiver.of(ios.getInputStream());
        this.clientRegRequester = RequesterAdaptersCollection.getStringRequester(Requester.fromSr(sender, receiver));
        this.jsonReceiver  = ReceiverAdaptersCollection.getJsonReceiver(receiver);
        this.switchDeser   = new MessageSwitchedDeserializer<>();
        switchDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            msg -> {
                pubCallback.accept(msg.body);
                return true;
            }
        );
        clientRegRequester.apply(clientId.toString());
    }
    private Client(Socket            clientSocket) {
        this(CloseableIos.getLazySocketIos(clientSocket));
    }

    synchronized private <T> void sendMessage(T                    object,
                                              SenderIf<Message<T>> sender) {
        var msg = new Message<T>();
        msg.id   = UUID.randomUUID();
        msg.body = object;
        msg.clientId = clientId;
        sender.send(msg);
    }
    synchronized private     void produce    (Publication          pub) {

        sendMessage(pub, senderIfs.pubSender);
    }
    synchronized private     void onConsumed (Method1<Publication> pubCallback) {

        if (!isConsuming()) {
            consume();
        }
        this.pubCallback = pubCallback;
    }

    public Client(InetSocketAddress serverAddr) {
        this(Util.getConnectedSocket(serverAddr));
    }

    @Override
    synchronized public final void            produce         (String topicName,
                                                               byte[] data) {

        var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = data;
        produce(pub);
    }
    @Override
    synchronized public final void            consume         () {

        jsonReceiver.recvWhile(switchDeser);
    }
    @Override
    synchronized public final Awaitable<Void> consumeStop     () {

        return jsonReceiver.recvStop();
    }
    @Override
    synchronized public final Boolean         isConsuming     () {

        return jsonReceiver.isReceiving();
    }
    @Override
    synchronized public final void            onConsumed      (Method2<String, byte[]> pubCallback) {

        onConsumed(pub -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }

    public final UUID getClientId     () { return clientId; }
    public final void close           () {

        sendMessage(new Close(), senderIfs.closeSender);
        closer.accept();
    }
    public final void subscribe       (SubscriptionByList  sub) {
        subscribeByList(sub.topicNames);
    }
    public final void subscribeByList (Set<String>         topicNames) {

        var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        sendMessage(sub, senderIfs.subListSender);
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
        sendMessage(sub, senderIfs.subReSender);
    }
    public final void subscribeByRegex(String              topicPattern) {

        subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final void subscribe       (SubscriptionToAll   sub) {
        subscribeToAll();
    }
    public final void subscribeToAll  () {

        sendMessage(new SubscriptionToAll(), senderIfs.subAllSender);
    }
    public final void subscribe       (SubscriptionToNone  sub) {
        subscribeToNone();
    }
    public final void subscribeToNone () {

        sendMessage(new SubscriptionToNone(), senderIfs.subNoneSender);
    }
}

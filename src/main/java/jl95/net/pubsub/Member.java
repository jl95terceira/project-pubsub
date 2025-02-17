package jl95.net.pubsub;

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
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.io.Receiver;
import jl95.net.io.ReceiverIf;
import jl95.net.io.collections.ReceiverAdaptersCollection;
import jl95.net.io.Sender;
import jl95.net.io.SenderIf;
import jl95.net.io.collections.SenderAdaptersCollections;
import jl95.net.io.util.Util;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.serdes.MessageSwitchedDeserializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.MessageType;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.io.CloseableIos;
import jl95.net.rpc.Requester;
import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.collections.RequesterAdaptersCollection;

public class Member implements MemberIf<byte[], byte[]> {

    private class BrokerIf {

        private final Sender                                sender;
        private final Receiver                              receiver;
        private final SenderIf<JsonValue>                   jsonSender;
        private final RequesterIf<String, String>           memberRegRequester;
        private final SenderIf<Message<Publication>>        pubSender;
        private final SenderIf<Message<Close>>              closeSender;
        private final SenderIf<Message<SubscriptionByList>> subListSender;
        private final SenderIf<Message<SubscriptionByRegex>>subReSender;
        private final SenderIf<Message<SubscriptionToAll>>  subAllSender;
        private final SenderIf<Message<SubscriptionToNone>> subNoneSender;
        private final ReceiverIf<JsonValue>                 jsonReceiver;

        public BrokerIf() {

            this.sender         = Sender.of(ios.getOutputStream());
            this.receiver       = Receiver.of(ios.getInputStream());
            this.jsonSender     = SenderAdaptersCollections.asJsonSender(sender);
            this.memberRegRequester = RequesterAdaptersCollection.asStringRequester(Requester.fromIo(ios));
            this.pubSender      = jsonSender.adaptedSender(SerdesDefaults.pubMsgToJson);
            this.closeSender    = jsonSender.adaptedSender(SerdesDefaults.closeReqToJson);
            this.subListSender  = jsonSender.adaptedSender(SerdesDefaults.subListReqToJson);
            this.subReSender    = jsonSender.adaptedSender(SerdesDefaults.subRegexReqToJson);
            this.subAllSender   = jsonSender.adaptedSender(SerdesDefaults.subAllReqToJson);
            this.subNoneSender  = jsonSender.adaptedSender(SerdesDefaults.subNoneReqToJson);
            this.jsonReceiver  = ReceiverAdaptersCollection.asJsonReceiver(receiver);
        }
    }

    private final CloseableIos                          ios;
    private final UUID                                  memberId = UUID.randomUUID();
    private final Method0                               closer;
    private final BrokerIf                              brokerIf;
    private final MessageSwitchedDeserializer<Boolean>  switchDeser;
    private       Method1<Publication>                  pubCallback = (pub) -> {/* pass */};

    private Member(CloseableIos      ios) {
        this.ios    = ios;
        this.closer = unchecked(ios::close);
        this.brokerIf = new BrokerIf();
        this.switchDeser   = new MessageSwitchedDeserializer<>();
        switchDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            msg -> {
                pubCallback.accept(msg.body);
                return true;
            }
        );
        brokerIf.memberRegRequester.apply(memberId.toString());
    }
    private Member(Socket            clientSocket) {
        this(CloseableIos.fromSocketLazy(clientSocket));
    }

    synchronized private <T> void sendMessage(T                    object,
                                              SenderIf<Message<T>> sender) {
        var msg = new Message<T>();
        msg.id       = UUID.randomUUID();
        msg.body     = object;
        msg.memberId = memberId;
        sender.send(msg);
    }
    synchronized private     void produce    (Publication          pub) {

        sendMessage(pub, brokerIf.pubSender);
    }
    synchronized private     void onConsumed (Method1<Publication> pubCallback) {

        if (!isConsuming()) {
            consume();
        }
        this.pubCallback = pubCallback;
    }

    public Member(InetSocketAddress serverAddr) {
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

        brokerIf.jsonReceiver.recvWhile(switchDeser);
    }
    @Override
    synchronized public final Awaitable<Void> consumeStop     () {

        return brokerIf.jsonReceiver.recvStop();
    }
    @Override
    synchronized public final Boolean         isConsuming     () {

        return brokerIf.jsonReceiver.isReceiving();
    }
    @Override
    synchronized public final void            onConsumed      (Method2<String, byte[]> pubCallback) {

        onConsumed(pub -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }

    public final UUID getMemberId() { return memberId; }
    public final void close           () {

        sendMessage(new Close(), brokerIf.closeSender);
        closer.accept();
    }
    public final void subscribe       (SubscriptionByList  sub) {
        subscribeByList(sub.topicNames);
    }
    public final void subscribeByList (Set<String>         topicNames) {

        var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        sendMessage(sub, brokerIf.subListSender);
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
        sendMessage(sub, brokerIf.subReSender);
    }
    public final void subscribeByRegex(String              topicPattern) {

        subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final void subscribe       (SubscriptionToAll   sub) {
        subscribeToAll();
    }
    public final void subscribeToAll  () {

        sendMessage(new SubscriptionToAll(), brokerIf.subAllSender);
    }
    public final void subscribe       (SubscriptionToNone  sub) {
        subscribeToNone();
    }
    public final void subscribeToNone () {

        sendMessage(new SubscriptionToNone(), brokerIf.subNoneSender);
    }
}

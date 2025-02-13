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
import jl95.net.Ios;
import jl95.net.Receiver;
import jl95.net.ReceiversCollection;
import jl95.net.Sender;
import jl95.net.SendersCollections;
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

public abstract class Client<P, C> {

    private final CloseableIos                          ios;
    private final UUID                                  clientId = UUID.randomUUID();
    private final Method0                               closer;
    private final Sender<Message<Publication>>          pubSender;
    private final Sender<Message<Close>>                closeSender;
    private final Sender<Message<SubscriptionByList>>   subListSender;
    private final Sender<Message<SubscriptionByRegex>>  subReSender;
    private final Sender<Message<SubscriptionToAll>>    subAllSender;
    private final Sender<Message<SubscriptionToNone>>   subNoneSender;
    private final Sender<String>                        stringSender;
    private final Receiver<JsonValue>                   jsonReceiver;
    private final MessageSwitchedDeserializer<Boolean>  switchDeser;
    private       Method1<Publication>                  pubCallback = (pub) -> {/* pass */};

    private Client(CloseableIos      ios) {
        this.ios    = ios;
        this.closer = unchecked(ios::close);
        var jsonSender     = SendersCollections.getJsonSender  (ios.getOutputStream());
        this.stringSender  = SendersCollections.getStringSender(ios.getOutputStream());
        this.pubSender     = jsonSender.adapted(SerdesDefaults.pubMsgToJson);
        this.closeSender   = jsonSender.adapted(SerdesDefaults.closeReqToJson);
        this.subListSender = jsonSender.adapted(SerdesDefaults.subListReqToJson);
        this.subReSender   = jsonSender.adapted(SerdesDefaults.subRegexReqToJson);
        this.subAllSender  = jsonSender.adapted(SerdesDefaults.subAllReqToJson);
        this.subNoneSender = jsonSender.adapted(SerdesDefaults.subNoneReqToJson);
        this.jsonReceiver  = ReceiversCollection.getJsonReceiver(ios.getInputStream());
        this.switchDeser   = new MessageSwitchedDeserializer<>();
        switchDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            msg -> {
                pubCallback.accept(msg.body);
                return true;
            }
        );
        stringSender.send(clientId.toString());
    }
    private Client(Socket            clientSocket) {
        this(CloseableIos.getLazySocketIos(clientSocket));
    }

    synchronized private <T> void sendMessage(T                    object,
                                              Sender<Message<T>>   sender) {
        var msg = new Message<T>();
        msg.id   = UUID.randomUUID();
        msg.body = object;
        msg.clientId = clientId;
        sender.send(msg);
    }
    synchronized private     void produce    (Publication          pub) {

        sendMessage(pub, pubSender);
    }
    synchronized private     void onConsumed (Method1<Publication> pubCallback) {

        if (!isConsuming()) {
            consume();
        }
        this.pubCallback = pubCallback;
    }

    protected abstract byte[] toBytes  (P      preSerial);
    protected abstract C      fromBytes(byte[] serial);

    public Client(InetSocketAddress serverAddr) {
        this(Util.getConnectedSocket(serverAddr));
    }

    synchronized public final void            produce         (String               topicName,
                                                               P                    data) {

        var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = toBytes(data);
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
    synchronized public final void            onConsumed      (Method2<String, C>   pubCallback) {

        onConsumed(pub -> {
            pubCallback.accept(pub.topicName, fromBytes(pub.data));
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

    public final <P2, C2> Client<P2, C2> adapted(Function1<P, P2> productionAdapter,
                                                 Function1<C2, C> consumptionAdapter) {
        return new Client<>(ios) {

            @Override
            protected byte[] toBytes(P2 preSerial) {
                return Client.this.toBytes(productionAdapter.apply(preSerial));
            }

            @Override
            protected C2 fromBytes(byte[] serial) {
                return consumptionAdapter.apply(Client.this.fromBytes(serial));
            }
        };
    }
}

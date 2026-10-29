package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.json.JsonValue;

import jl95.lang.I;
import jl95.net.io.CloseableIOStreamSupplier;
import jl95.net.io.ManagedIOStream;
import jl95.net.io.managed.RetriableIOStream;
import jl95.net.io.managed.SwitchingRetriableClientIOStream;
import jl95.net.io.managed.SwitchingRetriableIOStream;
import jl95.net.rpc.BytesIOSRRequester;
import jl95.net.rpc.Requester;
import jl95.net.rpc.switched.TypedRequester;
import jl95.util.StrictMap;
import jl95.lang.variadic.*;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationAcceptanceRequestJsonSerdes;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.MessageType;
import jl95.net.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.HelloJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.net.rpc.collections.TypedRequesterAdaptersCollection;
import jl95.net.rpc.collections.TypeSwitchedResponderAdaptersCollection;
import jl95.net.rpc.switched.TypeSwitchedResponderImpl;
import jl95.net.rpc.switched.TypeSwitchedResponder;
import jl95.net.rpc.switched.TypedRequesterImpl;
import jl95.util.UFuture;
import jl95.util.UVoidFuture;

public class Member implements Producer<JsonValue>, Consumer<JsonValue> {

    private static class RequestingFromBrokers {

        public final Requester<Message<Publication>,         Void> pubSender;
        public final Requester<Message<Close>,               Void> closeSender;
        public final Requester<Message<SubscriptionByList>,  Void> subListSender;
        public final Requester<Message<SubscriptionByRegex>, Void> subReSender;
        public final Requester<Message<SubscriptionToAll>,   Void> subAllSender;
        public final Requester<Message<SubscriptionToNone>,  Void> subNoneSender;

        public RequestingFromBrokers(ManagedIOStream ios) {

            final TypedRequester<JsonValue, Void>
                    jsonTypedRequester = TypedRequesterAdaptersCollection.asPostRequester(TypedRequesterImpl.fromManagedIo(ios));
            pubSender     = jsonTypedRequester.adaptedRequest(MessageSerializer.get(PublicationJsonSerdes        ::toJson))
                                              .getFunction   (MessageType.PUBLISH                  .value);
            closeSender   = jsonTypedRequester.adaptedRequest(MessageSerializer.get(CloseJsonSerdes              ::toJson))
                                              .getFunction   (MessageType.REQ_CLOSE                .value);
            subListSender = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionByListJsonSerdes ::toJson))
                                              .getFunction   (MessageType.REQ_SUBSCRIPTION_BY_LIST .value);
            subReSender   = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionByRegexJsonSerdes::toJson))
                                              .getFunction   (MessageType.REQ_SUBSCRIPTION_BY_REGEX.value);
            subAllSender  = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionToAllJsonSerdes  ::toJson))
                                              .getFunction   (MessageType.REQ_SUBSCRIPTION_TO_ALL  .value);
            subNoneSender = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionToNoneJsonSerdes ::toJson))
                                              .getFunction   (MessageType.REQ_SUBSCRIPTION_TO_NONE .value);
        }
    }
    private class RespondingToBrokers {

        public final TypeSwitchedResponder<JsonValue, JsonValue> switchedResponder;

        public RespondingToBrokers(ManagedIOStream ios) {

            this.switchedResponder = TypeSwitchedResponderImpl.fromIo(ios.get());
            switchedResponder
                .adaptedRequest (MessageDeserializer.get(PublicationAcceptanceRequestJsonSerdes::fromJson))
                .adaptedResponse(SerdesDefaults.boolToJson)
                .addCase(MessageType.PUBLISH_ACCEPT_REQUEST .value, x -> {
                    return true;
                });
            TypeSwitchedResponderAdaptersCollection.asPostResponder(switchedResponder)
                .adaptedRequest (MessageDeserializer.get(PublicationJsonSerdes::fromJson))
                .addCase(MessageType.PUBLISH                .value, x -> {
                    if (!acceptAccordingToCache(x)) {
                        return null;
                    }
                    updateCache(x);
                    pubCallback.accept(x.body);
                    return null;
                });
        }
    }

    public static Member of(Iterable<InetSocketAddress> addrs) {return new Member(addrs);}
    public static Member of(InetSocketAddress...        addrs) {return new Member(I(addrs));}

    private final UUID memberId = UUID.randomUUID();
    private final Iterable<InetSocketAddress> brokerAddrs;

    private Method0 closer;
    private SwitchingRetriableIOStream requestingFromBrokerIos;
    private RequestingFromBrokers requestingFromBrokers;
    private StrictMap<InetSocketAddress, RespondingToBrokers> respondingToBrokersMap;
    private Method0 connectFunction;
    private boolean connected = false;
    private Method1<Publication> pubCallback = pub -> {/* pass */};
    private Object gottenMessageIdsCacheSync = new Object();
    private int gottenMessageIdsCacheSize = 40;
    private Set<UUID> gottenMessageIdsCacheSet = new HashSet<>(gottenMessageIdsCacheSize);
    private Deque<UUID> gottenMessageIdsCacheQueue = new LinkedList<>();

    private synchronized <A, R> UFuture<R> postget(A object, Requester<Message<A>, R> sender) {
        final Message<A> msg = new Message<>();
        msg.id       = UUID.randomUUID();
        msg.body     = object;
        msg.memberId = memberId;
        return sender.apply(msg);
    }
    private synchronized UVoidFuture produce(Publication pub) {
        return postget(pub, requestingFromBrokers.pubSender).voided();
    }
    private synchronized void onConsumed(Method1<Publication> pubCallback) {

        this.pubCallback = pubCallback;
    }

    protected Member(Iterable<InetSocketAddress> brokerAddrs) {

        this.brokerAddrs = brokerAddrs;
    }

    private boolean acceptAccordingToCache(Message<Publication> message) {
        synchronized (gottenMessageIdsCacheSync) {
            return !gottenMessageIdsCacheSet.contains(message.id);
        }
    }
    private void updateCache(UUID messageId) {
        synchronized (gottenMessageIdsCacheSync) {
            while (gottenMessageIdsCacheSet.size() >= gottenMessageIdsCacheSize) /* make room for at least 1 element */ {
                final boolean removeFirstSuccessful = gottenMessageIdsCacheSet.remove(gottenMessageIdsCacheQueue.removeFirst());
                if (!removeFirstSuccessful) {
                    //TODO: call a handler so that this case does not pass silently
                    return;
                }
            }
            gottenMessageIdsCacheSet.add(messageId);
            gottenMessageIdsCacheQueue.addLast(messageId);
        }
    }
    private void updateCache(Message<Publication> message) {
        updateCache(message.id);
    }

    public synchronized final void connect() {

        final Map<InetSocketAddress, java.net.Socket>
                responsesSocketMap = I.of(brokerAddrs).toMap(a -> a, jl95.net.Util::getSocketByConnect);
        requestingFromBrokerIos = SwitchingRetriableClientIOStream.of(brokerAddrs /* TODO: handle each address and IO on (re)connect */);
        final Map<InetSocketAddress, ManagedIOStream>
                responsesIosMap = I.of(responsesSocketMap.entrySet()).toMap(Map.Entry::getKey, e -> ManagedIOStream.of(CloseableIOStreamSupplier.fromSocketLazy(e.getValue())));
        closer = unchecked(() -> {
            requestingFromBrokerIos.close();
            for (final ManagedIOStream responsesIos: responsesIosMap.values()) {
                responsesIos.get().close();
            }
        });
        requestingFromBrokers = new RequestingFromBrokers(requestingFromBrokerIos);
        respondingToBrokersMap = strict(I.of(responsesIosMap.entrySet()).toMap(Map.Entry::getKey, e -> new RespondingToBrokers(e.getValue())));
        final Requester<Message<Hello>, JsonValue> requestsHelloRequester = BytesIOSRRequester.of(requestingFromBrokerIos)
                .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
        postget(new Hello(Hello.Type.MEMBER_REQUESTS),  requestsHelloRequester)
                .get(1, TimeUnit.MINUTES);
        for (final ManagedIOStream responsesIos: responsesIosMap.values()) {
            final Requester<Message<Hello>, JsonValue> responsesHelloRequester = BytesIOSRRequester.of(responsesIos)
                    .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                    .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
            postget(new Hello(Hello.Type.MEMBER_RESPONSES), responsesHelloRequester)
                    .get(1, TimeUnit.MINUTES);
        }
        connected = true;
    }

    public final void close() {

        requestingFromBrokerIos.setRetryLimit(0);
        if (connected) {
            try {
                postget(new Close(), requestingFromBrokers.closeSender);
            }
            catch (RetriableIOStream.NoMoreRetriesException ex) {}
        }
        closer.accept();
    }
    public final UUID getMemberId() { return memberId; }
    public final UVoidFuture subscribe       (SubscriptionByList sub) {

        return postget(sub, requestingFromBrokers.subListSender).voided();
    }
    public final UVoidFuture subscribeByList (Set<String> topicNames) {

        final var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        return subscribe(sub);
    }
    public final UVoidFuture subscribeByList (Iterable<String>    topicNames) {

        return subscribeByList(I.of(topicNames).toSet());
    }
    public final UVoidFuture subscribe       (SubscriptionByRegex sub) {

        return postget(sub, requestingFromBrokers.subReSender).voided();
    }
    public final UVoidFuture subscribeByRegex(Pattern             topicPattern) {

        final var sub = new SubscriptionByRegex();
        sub.topicPattern = topicPattern;
        return subscribe(sub);
    }
    public final UVoidFuture subscribeByRegex(String              topicPattern) {

        return subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final UVoidFuture subscribe       (SubscriptionToAll   sub) {

        return postget(sub, requestingFromBrokers.subAllSender).voided();
    }
    public final UVoidFuture subscribeToAll  () {

        return subscribe(new SubscriptionToAll());
    }
    public final UVoidFuture subscribe       (SubscriptionToNone  sub) {

        return postget(sub, requestingFromBrokers.subNoneSender).voided();
    }
    public final UVoidFuture subscribeToNone () {

        return subscribe(new SubscriptionToNone());
    }
    public final void resizeCache(int newSize) {
        gottenMessageIdsCacheSize = newSize;
        gottenMessageIdsCacheSet = new HashSet<>(gottenMessageIdsCacheSize);
        final Deque<UUID> dequeOld = gottenMessageIdsCacheQueue;
        gottenMessageIdsCacheQueue = new LinkedList<>();
        while (!dequeOld.isEmpty()) {
            updateCache(dequeOld.removeFirst());
        }
    }

    @Override public synchronized final UVoidFuture produce    (String topicName, JsonValue data) {

        final var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = data;
        return produce(pub);
    }
    @Override public synchronized final void        consume    () {
        for (var responderIf: respondingToBrokersMap.values()) {
            responderIf.switchedResponder.start();
        }
    }
    @Override public synchronized final UVoidFuture consumeStop() {

        final var futures = I.of(respondingToBrokersMap.values()).map(r -> r.switchedResponder.stop()).toList();
        return UVoidFuture.joined(futures);
    }
    @Override public synchronized final Boolean     isConsuming() {

        return I.all(I.of(respondingToBrokersMap.values()).map(r -> r.switchedResponder.isRunning()));
    }
    @Override public synchronized final void        onConsumed (Method2<String, JsonValue> pubCallback) {

        onConsumed((Publication pub) -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }
}

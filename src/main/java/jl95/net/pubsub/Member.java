package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.json.JsonValue;

import jl95.lang.I;
import jl95.net.rpc.Requester;
import jl95.net.rpc.RequesterIf;
import jl95.util.StrictMap;
import jl95.lang.variadic.*;
import jl95.net.io.managed.ManagedIos;
import jl95.net.io.managed.RetriableIos;
import jl95.net.io.managed.SwitchingRetriableClientIos;
import jl95.net.io.managed.SwitchingRetriableIos;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.io.Util;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationAcceptanceRequestJsonSerdes;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.MessageType;
import jl95.net.io.CloseableIos;
import jl95.net.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.HelloJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.net.rpc.collections.TypedRequesterAdaptersCollection;
import jl95.net.rpc.collections.TypeSwitchedResponderAdaptersCollection;
import jl95.net.rpc.switched.TypeSwitchedResponder;
import jl95.net.rpc.switched.TypeSwitchedResponderIf;
import jl95.net.rpc.switched.TypedRequester;
import jl95.util.UFuture;
import jl95.util.UVoidFuture;

public class Member implements MemberIf<JsonValue, JsonValue> {

    private static class Requesting {

        public final RequesterIf<Message<Publication>,         Void> pubSender;
        public final RequesterIf<Message<Close>,               Void> closeSender;
        public final RequesterIf<Message<SubscriptionByList>,  Void> subListSender;
        public final RequesterIf<Message<SubscriptionByRegex>, Void> subReSender;
        public final RequesterIf<Message<SubscriptionToAll>,   Void> subAllSender;
        public final RequesterIf<Message<SubscriptionToNone>,  Void> subNoneSender;

        public Requesting(ManagedIos ios) {

            var jsonTypedRequester = TypedRequesterAdaptersCollection.asPostRequester(TypedRequester.fromManagedIo(ios));
            this.pubSender       = jsonTypedRequester.adaptedRequest(MessageSerializer.get(PublicationJsonSerdes        ::toJson))
                                             .getFunction   (MessageType.PUBLISH                  .value);
            this.closeSender     = jsonTypedRequester.adaptedRequest(MessageSerializer.get(CloseJsonSerdes              ::toJson))
                                             .getFunction   (MessageType.REQ_CLOSE                .value);
            this.subListSender   = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionByListJsonSerdes ::toJson))
                                             .getFunction   (MessageType.REQ_SUBSCRIPTION_BY_LIST .value);
            this.subReSender     = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionByRegexJsonSerdes::toJson))
                                             .getFunction   (MessageType.REQ_SUBSCRIPTION_BY_REGEX.value);
            this.subAllSender    = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionToAllJsonSerdes  ::toJson))
                                             .getFunction   (MessageType.REQ_SUBSCRIPTION_TO_ALL  .value);
            this.subNoneSender   = jsonTypedRequester.adaptedRequest(MessageSerializer.get(SubscriptionToNoneJsonSerdes ::toJson))
                                             .getFunction   (MessageType.REQ_SUBSCRIPTION_TO_NONE .value);
        }
    }
    private class Responding {

        public final TypeSwitchedResponderIf<JsonValue, JsonValue> switchedResponder;

        public Responding(ManagedIos ios) {

            this.switchedResponder = TypeSwitchedResponder.fromIo(ios.getIo());
            switchedResponder
                .adaptedRequest (MessageDeserializer.get(PublicationAcceptanceRequestJsonSerdes::fromJson))
                .adaptedResponse(SerdesDefaults.boolToJson)
                .addCase(MessageType.PUBLISH_ACCEPT_REQUEST .value, x -> {
                    return true;
                });
            TypeSwitchedResponderAdaptersCollection.asPostResponder(switchedResponder)
                .adaptedRequest (MessageDeserializer.get(PublicationJsonSerdes::fromJson))
                .addCase(MessageType.PUBLISH                .value, x -> {
                    pubCallback.accept(x.body);
                    return null;
                });
        }
    }

    public static Member of(Iterable<InetSocketAddress> addrs) {return new Member(addrs);}
    public static Member of(InetSocketAddress...        addrs) {return new Member(I(addrs));}

    private final UUID                  memberId = UUID.randomUUID();
    private final Method0               closer;
    private final SwitchingRetriableIos requestsIos;
    private final Requesting            requesterIf;
    private final StrictMap<InetSocketAddress,Responding> responderIfMap;
    private final Method0               connectFunction;
    private       Boolean               connected   = false;
    private       Method1<Publication>  pubCallback = (pub) -> {/* pass */};

    synchronized private <A, R> R postget       (A object, RequesterIf<Message<A>, R> sender, Function1<R, UFuture<R>> futureResolver) {
        var msg = new Message<A>();
        msg.id       = UUID.randomUUID();
        msg.body     = object;
        msg.memberId = memberId;
        return futureResolver.apply(sender.apply(msg));
    }
    synchronized private <A, R> R postget       (A object, RequesterIf<Message<A>, R> sender) {
        return postget(object, sender, f -> f.get(10, TimeUnit.SECONDS));
    }
    synchronized private     void produce       (Publication pub) {

        postget(pub, requesterIf.pubSender);
    }
    synchronized private     void onConsumed    (Method1<Publication> pubCallback) {

        this.pubCallback = pubCallback;
    }

    private Member(Iterable<InetSocketAddress> brokerAddrs) {

        var responsesSocketMap  = I.of(brokerAddrs).toMap(a -> a, jl95.net.Util::getSocketByConnect);
        requestsIos             = SwitchingRetriableClientIos.of(brokerAddrs);
        var responsesIosMap     = I.of(responsesSocketMap.entrySet()).toMap(Map.Entry::getKey, e -> ManagedIos.of(CloseableIos.fromSocketLazy(e.getValue())));
        this.closer             = unchecked(() -> {
            requestsIos.close();
            for (var responsesIos: responsesIosMap.values()) {
                responsesIos.getInputStream().close();
            }
        });
        this.requesterIf        = new Requesting(requestsIos);
        this.responderIfMap     = strict(I.of(responsesIosMap.entrySet()).toMap(Map.Entry::getKey, e -> new Responding(e.getValue())));
        connectFunction         = () -> {
            var requestsHelloRequester  = Requester.fromManagedIo(requestsIos)
                .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
            postget(new Hello(Hello.Type.MEMBER_REQUESTS),  requestsHelloRequester);
            for (var responsesIos: responsesIosMap.values()) {
                var responsesHelloRequester = Requester.fromManagedIo(responsesIos)
                        .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
                postget(new Hello(Hello.Type.MEMBER_RESPONSES), responsesHelloRequester);
            }
        };
    }

    synchronized
    public final void connect         () {

        connectFunction.accept();
        connected = true;
    }
    public final UUID getMemberId     () { return memberId; }
    public final void close           () {

        requestsIos.setRetryLimit(0);
        if (connected) {
            try {
                postget(new Close(), requesterIf.closeSender, f -> f.get(10, TimeUnit.SECONDS));
            }
            catch (RetriableIos.NoMoreRetriesException ex) {}
        }
        closer.accept();
    }
    public final void subscribe       (SubscriptionByList  sub) {

        postget(sub, requesterIf.subListSender);
    }
    public final void subscribeByList (Set<String>         topicNames) {

        var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        subscribe(sub);
    }
    public final void subscribeByList (Iterable<String>    topicNames) {

        subscribeByList(I.of(topicNames).toSet());
    }
    public final void subscribe       (SubscriptionByRegex sub) {

        postget(sub, requesterIf.subReSender);
    }
    public final void subscribeByRegex(Pattern             topicPattern) {

        var sub = new SubscriptionByRegex();
        sub.topicPattern = topicPattern;
        subscribe(sub);
    }
    public final void subscribeByRegex(String              topicPattern) {

        subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final void subscribe       (SubscriptionToAll   sub) {

        postget(sub, requesterIf.subAllSender);
    }
    public final void subscribeToAll  () {

        subscribe(new SubscriptionToAll());
    }
    public final void subscribe       (SubscriptionToNone  sub) {

        postget(sub, requesterIf.subNoneSender);
    }
    public final void subscribeToNone () {

        subscribe(new SubscriptionToNone());
    }

    @Override
    synchronized public final void        produce         (String topicName,
                                                           JsonValue data) {

        var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = data;
        produce(pub);
    }
    @Override
    synchronized public final void        consume         () {
        for (var responderIf: responderIfMap.values()) {
            responderIf.switchedResponder.start();
        }
    }
    @Override
    synchronized public final UVoidFuture consumeStop     () {

        var futures = I.of(responderIfMap.values()).map(r -> r.switchedResponder.stop()).toList();
        return UVoidFuture.joined(futures);
    }
    @Override
    synchronized public final Boolean     isConsuming     () {

        return I.all(I.of(responderIfMap.values()).map(r -> r.switchedResponder.isRunning()));
    }
    @Override
    synchronized public final void        onConsumed      (Method2<String, JsonValue> pubCallback) {

        onConsumed((Publication pub) -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }
}

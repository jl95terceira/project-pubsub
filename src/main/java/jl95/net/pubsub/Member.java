package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.I;
import jl95.lang.StrictMap;
import jl95.lang.variadic.*;
import jl95.net.io.Ios;
import jl95.net.io.SenderReceiverIf;
import jl95.net.io.managed.ManagedIos;
import jl95.net.io.managed.SwitchingRetriableClientIos;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.io.util.Util;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.MessageType;
import jl95.net.io.CloseableIos;
import jl95.net.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.HelloJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.net.rpc.Requester;
import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.Responder;
import jl95.net.rpc.ResponderIf;
import jl95.net.rpc.collections.RequesterAdaptersCollection;
import jl95.net.rpc.collections.ResponderAdaptersCollection;
import jl95.net.rpc.switched.TypedRequester;
import jl95.net.rpc.util.Defaults;

public class Member implements MemberIf<JsonValue, JsonValue> {

    private static class Requesting {

        public final jl95.net.rpc.RequesterIf<Message<Publication>,         Void> pubSender;
        public final jl95.net.rpc.RequesterIf<Message<Close>,               Void> closeSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionByList>,  Void> subListSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionByRegex>, Void> subReSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionToAll>,   Void> subAllSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionToNone>,  Void> subNoneSender;

        public Requesting(ManagedIos ios) {

            var jsonTypedRequester = RequesterAdaptersCollection.asPostRequester(TypedRequester.fromManagedIo(ios));
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
    private static class Responding {

        public final ResponderIf<JsonValue, Void> jsonReceiver;

        public Responding(ManagedIos ios) {

            var sr = SenderReceiverIf.fromManagedIo(ios);
            this.jsonReceiver = ResponderAdaptersCollection.asPostResponder(Responder.fromSr(sr));
        }
    }

    private final UUID                  memberId = UUID.randomUUID();
    private final Method0               closer;
    private final Requesting            requesterIf;
    private final StrictMap<InetSocketAddress,Responding> responderIfMap;
    private final Method0               connectFunction;
    private final RequesterIf.SendOptions.Editable sendOptions = new RequesterIf.SendOptions.Editable();
    private       Method1<Publication>  pubCallback = (pub) -> {/* pass */};

    synchronized private <A, R> R postget       (A object, RequesterIf<Message<A>, R> sender) {
        var msg = new Message<A>();
        msg.id       = UUID.randomUUID();
        msg.body     = object;
        msg.memberId = memberId;
        return sender.apply(msg, sendOptions);
    }
    synchronized private     void produce       (Publication pub) {

        postget(pub, requesterIf.pubSender);
    }
    synchronized private     void onConsumed    (Method1<Publication> pubCallback) {

        this.pubCallback = pubCallback;
    }

    public Member(InetSocketAddress... brokerAddrs) {

        var responsesSocketMap = I(brokerAddrs).toMap(a -> a, Util::getSocketByConnect);
        var requestsIos     = SwitchingRetriableClientIos.of(brokerAddrs);
        var responsesIosMap = I.of(responsesSocketMap.entrySet()).toMap(Map.Entry::getKey, e -> ManagedIos.of(CloseableIos.fromSocketLazy(e.getValue())));
        this.closer   = unchecked(() -> {
            requestsIos .getIo().close();
            for (var responsesIos: responsesIosMap.values()) {
                responsesIos.getIo().close();
            }
        });
        this.requesterIf    = new Requesting(requestsIos);
        this.responderIfMap = strict(I.of(responsesIosMap.entrySet()).toMap(Map.Entry::getKey, e -> new Responding(e.getValue())));
        connectFunction = () -> {
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
    }
    public final UUID getMemberId     () { return memberId; }
    public final void close           () {

        postget(new Close(), requesterIf.closeSender);
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
    synchronized public final void            produce         (String topicName,
                                                               JsonValue data) {

        var pub = new Publication();
        pub.topicName = topicName;
        pub.data      = data;
        produce(pub);
    }
    @Override
    synchronized public final void            consume         () {
        for (var responderIf: responderIfMap.values())
        responderIf.jsonReceiver.respond(json -> {
            pubCallback.accept(MessageDeserializer.get(PublicationJsonSerdes::fromJson).apply(json).body);
            return null;
        });
    }
    @Override
    synchronized public final Awaitable<Void> consumeStop     () {

        var futures = I.of(responderIfMap.values()).map(r -> r.jsonReceiver.stop()).toList();
        return new Awaitable<Void>() {
            @Override public Void await() {
                for (var future: futures) future.await();
                return null;
            }
            @Override public Boolean isDone() {
                return I.all(I.of(futures).map(Awaitable::isDone));
            }
        };
    }
    @Override
    synchronized public final Boolean         isConsuming     () {

        return I.all(I.of(responderIfMap.values()).map(r -> r.jsonReceiver.isRunning()));
    }
    @Override
    synchronized public final void            onConsumed      (Method2<String, JsonValue> pubCallback) {

        onConsumed(pub -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }
}

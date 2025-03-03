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
import jl95.net.io.Ios;
import jl95.net.io.SenderReceiverIf;
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

public class Member implements MemberIf<JsonValue, JsonValue> {

    private static class Requesting {

        public final jl95.net.rpc.RequesterIf<Message<Publication>,         Void> pubSender;
        public final jl95.net.rpc.RequesterIf<Message<Close>,               Void> closeSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionByList>,  Void> subListSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionByRegex>, Void> subReSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionToAll>,   Void> subAllSender;
        public final jl95.net.rpc.RequesterIf<Message<SubscriptionToNone>,  Void> subNoneSender;

        public Requesting(Ios ios) {

            var jsonTypedRequester = RequesterAdaptersCollection.asPostRequester(TypedRequester.fromIo(ios));
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

        public Responding(Ios ios) {

            var sr = SenderReceiverIf.fromIo(ios);
            this.jsonReceiver = ResponderAdaptersCollection.asPostResponder(Responder.fromSr(sr));
        }
    }

    private final UUID                  memberId = UUID.randomUUID();
    private final Method0               closer;
    private final Requesting            requesterIf;
    private final Responding            responderIf;
    private       Method1<Publication>  pubCallback = (pub) -> {/* pass */};

    private Member(CloseableIos requestsIos,
                   CloseableIos responsesIos) {

        this.closer   = unchecked(() -> {
            requestsIos.close();
            responsesIos.close();
        });
        this.requesterIf = new Requesting(requestsIos);
        this.responderIf = new Responding(responsesIos);
        var requestsHelloRequester  = Requester.fromIo(requestsIos)
            .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
        var responsesHelloRequester = Requester.fromIo(responsesIos)
            .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
        postget(new Hello(Hello.Type.MEMBER_REQUESTS),  requestsHelloRequester);
        postget(new Hello(Hello.Type.MEMBER_RESPONSES), responsesHelloRequester);
    }
    private Member(Socket       requestsSocket,
                   Socket       responsesSocket) {

        this(CloseableIos.fromSocketLazy(requestsSocket),
             CloseableIos.fromSocketLazy(responsesSocket));
    }

    synchronized private <A, R> R postget       (A object, RequesterIf<Message<A>, R> sender) {
        var msg = new Message<A>();
        msg.id       = UUID.randomUUID();
        msg.body     = object;
        msg.memberId = memberId;
        return sender.apply(msg);
    }
    synchronized private     void produce       (Publication pub) {

        postget(pub, requesterIf.pubSender);
    }
    synchronized private     void onConsumed    (Method1<Publication> pubCallback) {

        this.pubCallback = pubCallback;
    }

    public Member(InetSocketAddress brokerAddr) {
        this(Util.getSocketByConnect(brokerAddr), Util.getSocketByConnect(brokerAddr));
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
        responderIf.jsonReceiver.respond(json -> {
            pubCallback.accept(MessageDeserializer.get(PublicationJsonSerdes::fromJson).apply(json).body);
            return null;
        });
    }
    @Override
    synchronized public final Awaitable<Void> consumeStop     () {

        return responderIf.jsonReceiver.stop();
    }
    @Override
    synchronized public final Boolean         isConsuming     () {

        return responderIf.jsonReceiver.isRunning();
    }
    @Override
    synchronized public final void            onConsumed      (Method2<String, JsonValue> pubCallback) {

        onConsumed(pub -> {
            pubCallback.accept(pub.topicName, pub.data);
        });
    }

    public final UUID getMemberId     () { return memberId; }
    public final void close           () {

        postget(new Close(), requesterIf.closeSender);
        closer.accept();
    }
    public final void subscribe       (SubscriptionByList  sub) {
        subscribeByList(sub.topicNames);
    }
    public final void subscribeByList (Set<String>         topicNames) {

        var sub = new SubscriptionByList();
        sub.topicNames = topicNames;
        postget(sub, requesterIf.subListSender);
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
        postget(sub, requesterIf.subReSender);
    }
    public final void subscribeByRegex(String              topicPattern) {

        subscribeByRegex(Pattern.compile(topicPattern));
    }
    public final void subscribe       (SubscriptionToAll   sub) {
        subscribeToAll();
    }
    public final void subscribeToAll  () {

        postget(new SubscriptionToAll(), requesterIf.subAllSender);
    }
    public final void subscribe       (SubscriptionToNone  sub) {
        subscribeToNone();
    }
    public final void subscribeToNone () {

        postget(new SubscriptionToNone(), requesterIf.subNoneSender);
    }
}

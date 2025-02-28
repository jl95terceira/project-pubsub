package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import jl95.lang.Awaitable;
import jl95.lang.StrictMap;
import jl95.net.io.CloseableIos;
import jl95.net.io.Ios;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.BrokerRequestsConnection;
import jl95.net.pubsub.util.BrokerResponsesConnection;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.MemberResponsesConnection;
import jl95.net.pubsub.util.MemberRequestsConnection;
import jl95.net.io.util.Util;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.protocol.HelloJsonSerdes;
import jl95.net.rpc.Requester;
import jl95.net.rpc.Responder;
import jl95.net.rpc.collections.RequesterAdaptersCollection;
import jl95.net.rpc.collections.ResponderAdaptersCollection;

public class Broker {

    interface RequestHandler<T> extends Function1<Boolean, Message<T>> {}

    private final UUID brokerId = UUID.randomUUID();
    private final StrictMap<UUID, MemberRequestsConnection>  memberRequestsMap  = StrictMap.of(new ConcurrentHashMap<>());
    private final StrictMap<UUID, MemberResponsesConnection> memberResponsesMap = StrictMap.of(new ConcurrentHashMap<>());
    private final StrictMap<UUID, BrokerRequestsConnection>  brokerRequestsMap  = StrictMap.of(new ConcurrentHashMap<>());
    private final StrictMap<UUID, BrokerResponsesConnection> brokerResponsesMap = StrictMap.of(new ConcurrentHashMap<>());
    private final Object                                     brokerLinkSync     = new Object();
    private final StrictMap<UUID, Subscription>              subscriptionsMap   = StrictMap.of(new ConcurrentHashMap<>());

    private final jl95.net.Server netServer;

    public Broker(ServerSocket      socket) {
        this.netServer = new jl95.net.Server(socket);
        this.netServer.setAcceptCb((self, socket_) -> onAccept(socket_));
    }
    public Broker(InetSocketAddress addr) {
        this(Util.getSimpleServerSocket(addr));
    }

    private void                        onAccept             (Socket socket) {
        var memberIdFuture = new CompletableFuture<UUID>();
        var helloTypeFuture  = new CompletableFuture<Hello.Type>();
        var helloResponder   = ResponderAdaptersCollection.asPostResponder(Responder.fromIo(Ios.fromSocket(socket))).adaptedRequest(
                MessageDeserializer.get(HelloJsonSerdes::fromJson));
        var acceptedFuture = new CompletableFuture<Void>();
        helloResponder.respondOnce(hello -> {
            helloTypeFuture .complete(hello.body.type);
            memberIdFuture.complete(hello.memberId);
            uncheck(() -> acceptedFuture.get());
            return null;
        }).await();
        var helloType = uncheck(() -> helloTypeFuture .get());
        var memberId  = uncheck(() -> memberIdFuture.get());
        switch (helloType) {
            case MEMBER_REQUESTS -> {
                var connection = new MemberRequestsConnection(socket);
                memberRequestsMap.put(memberId, connection);
                connection.closeReqHandler    = decorated(getCloseReqHandler(memberId), msg -> cbs -> {});
                connection.subListReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubList (msg));
                connection.subRegexReqHandler = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubRegex(msg));
                connection.subAllReqHandler   = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubAll  (msg));
                connection.subNoneReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubNone (msg));
                connection.pubReqHandler      = decorated(getPubReqHandler  (memberId), msg -> cbs -> cbs.onPub     (msg));
                connection.startRespond().await();
            }
            case MEMBER_RESPONSES -> {
                var connection = new MemberResponsesConnection(socket);
                memberResponsesMap.put(memberId, connection);
                connection.startQueueLoop();
            }
            case BROKER_REQUESTS -> {
                synchronized (brokerLinkSync) {
                    if (brokerRequestsMap .containsKey(memberId)) break;
                    var connection = new BrokerRequestsConnection(socket);
                    brokerRequestsMap.put(memberId, connection);
                    connection.closeReqHandler    = decorated(getCloseReqHandler(memberId), msg -> cbs -> {});
                    connection.subListReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubList (msg));
                    connection.subRegexReqHandler = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubRegex(msg));
                    connection.subAllReqHandler   = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubAll  (msg));
                    connection.subNoneReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubNone (msg));
                    connection.pubReqHandler      = decorated(getPubReqHandler  (memberId), msg -> cbs -> cbs.onPub     (msg));
                    connection.startRespond().await();
                }
            }
            case BROKER_RESPONSES -> {
                synchronized (brokerLinkSync) {
                    if (brokerResponsesMap.containsKey(memberId)) break;
                    var connection = new BrokerResponsesConnection(socket);
                    brokerResponsesMap.put(memberId, connection);
                    connection.startQueueLoop();
                }
            }
            default -> throw new AssertionError();
        }
        acceptedFuture.complete(null);
        helloResponder.stop().await();
        assert !helloResponder.isRunning();
    }
    private void                        closeMemberConnection(UUID memberId) {
        memberRequestsMap .get   (memberId).close();
        memberRequestsMap .remove(memberId);
        memberResponsesMap.get   (memberId).close();
        memberResponsesMap.remove(memberId);
    }
    private <T>

            RequestHandler<T>          decorated         (RequestHandler<T> handler, Function1<Method1<BrokerResponsesConnection.Callbacks>, Message<T>> brokerCbCallerSupplier) {
        return msg -> {
            msg.stamps.add(getBrokerId());
            var re = handler.apply(msg);
            propagateReq(msg, brokerCbCallerSupplier.apply(msg));
            return re;
        };
    }
    private RequestHandler<Close>      getCloseReqHandler(UUID entityId) { return msg -> !entityId.equals(msg.memberId); }
    private <S extends Subscription>
            RequestHandler<S>          getSubReqHandler  (UUID entityId) {
        return msg -> {
            setSubscription(msg.memberId, msg.body);
            return true;
        };
    }
    private RequestHandler<Publication>getPubReqHandler  (UUID entityId) {
        return msg -> {
            var pub = msg.body;
            for (var UUIDOfOther: subscriptionsMap.keySet()) {
                if (getSubscription(UUIDOfOther).accepts(pub.topicName)) {
                    if (memberResponsesMap.containsKey(UUIDOfOther)) {
                        memberResponsesMap.get(UUIDOfOther).addToQueue(msg);
                    }
                }
            }
            return true;
        };
    }
    private <T>
            void                       propagateReq      (Message<T> msg, Method1<BrokerResponsesConnection.Callbacks> brokerCbCaller) {
        for (var e: brokerResponsesMap.entrySet()) {
            var brokerId         = e.getKey  ();
            var brokerConnection = e.getValue();
            if (msg.stamps.contains(brokerId)) continue;
            brokerConnection.addToQueue(brokerCbCaller);
        }
    }
    private void                       linkBroker        (Socket requestsSocket,
                                                          Socket responsesSocket) {
        synchronized (brokerLinkSync) {
            var requestsIos  = CloseableIos.fromSocketLazy(requestsSocket);
            var responsesIos = CloseableIos.fromSocketLazy(responsesSocket);
            var requestsHelloRequester = RequesterAdaptersCollection
                .asPostRequester(Requester.fromIo(requestsIos))
                .adaptedRequest(MessageSerializer.get(HelloJsonSerdes::toJson));
            var responsesHelloRequester = RequesterAdaptersCollection
                .asPostRequester(Requester.fromIo(responsesIos))
                .adaptedRequest (MessageSerializer.get(HelloJsonSerdes::toJson));
            for (var t: I(
                tuple(Hello.Type.BROKER_REQUESTS,  requestsHelloRequester),
                tuple(Hello.Type.BROKER_RESPONSES, responsesHelloRequester)
            )) {
                var helloMsg = new Message<Hello>();
                helloMsg.id       = UUID.randomUUID();
                helloMsg.body     = new Hello(t.a1);
                helloMsg.memberId = brokerId;
                t.a2.apply(helloMsg);
            }
        }
    }

    public final Awaitable<Void>            startAccept     () {

        return netServer.start();
    }
    public final Awaitable<Void>            stopAccept      () {

        return netServer.stop();
    }
    public final UUID                       getBrokerId     () {
        return brokerId;
    }
    public final InetSocketAddress          getBrokerAddress() {
        var socket = getNetServer().getSocket();
        return new InetSocketAddress(socket.getInetAddress(), socket.getLocalPort());
    }
    public final Iterable<UUID>             getMemberIds    () {

        return I.of(memberRequestsMap.keySet());
    }
    public final Iterable<InetSocketAddress>getAddresses    () {

        return I.of(memberRequestsMap.values())
                 .map(MemberRequestsConnection::getSocket)
                 .map(socket -> new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }
    public final Subscription               getSubscription (UUID memberId) {

        return subscriptionsMap.get(memberId);
    }
    public final void                       setSubscription (UUID memberId,
                                                             Subscription subscription) {

        subscriptionsMap.put(memberId, subscription);
   }
    public final void                       linkBroker      (InetSocketAddress addr) {
        linkBroker(Util.getConnectedSocket(addr),
                   Util.getConnectedSocket(addr));
    }
    public final void                       closeConnections() {

        for (var clientId: memberRequestsMap.keySet()) {
            closeMemberConnection(clientId);
        }
    }
    public final jl95.net.Server            getNetServer    () {

        return netServer;
    }
    public final void                       close           () {
        closeConnections();
        getNetServer().close();
    }
}

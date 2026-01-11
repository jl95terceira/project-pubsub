package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import jl95.net.rpc.Requester;
import jl95.net.rpc.Responder;
import jl95.util.*;
import jl95.net.io.CloseableIos;
import jl95.net.io.Ios;
import jl95.net.pubsub.listen.UpdateSubscription;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.BrokerRequestsConnection;
import jl95.net.pubsub.util.BrokerResponsesConnection;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.MemberResponsesConnection;
import jl95.net.pubsub.util.MemberRequestsConnection;
import jl95.net.io.Util;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.net.pubsub.util.SerdesDefaults;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.protocol.HelloJsonSerdes;

public class Broker {

    interface RequestHandler<T> extends Function1<Boolean, Message<T>> {}

    // id
    private final UUID brokerId = UUID.randomUUID();
    // pub-sub management
    private final StrictMap<UUID, MemberRequestsConnection>  memberRequestsMap  = strict(new ConcurrentHashMap<>());
    private final StrictMap<UUID, MemberResponsesConnection> memberResponsesMap = strict(new ConcurrentHashMap<>());
    private final StrictMap<UUID, BrokerRequestsConnection>  brokerRequestsMap  = strict(new ConcurrentHashMap<>());
    private final StrictMap<UUID, BrokerResponsesConnection> brokerResponsesMap = strict(new ConcurrentHashMap<>());
    private final Object                                     brokerLinkSync     = new Object();
    private final StrictMap<UUID, Subscription>              subscriptionsMap   = strict(new ConcurrentHashMap<>());
    // networking
    private final jl95.net.Server netServer;
    // listeners
    private final StrictMap <UUID, Method1<UpdateSubscription>> listenersOnUpdateSubscriptionMap        = strict(new ConcurrentHashMap<>());
    private final AutoMapper<UUID, Method1<UpdateSubscription>> listenersOnUpdateSubscriptionAutoMapper = AutoMappersCollection.getUuidAutoMapper(listenersOnUpdateSubscriptionMap);

    public Broker(ServerSocket      socket) {
        this.netServer = jl95.net.Server.fromSocket(socket);
        this.netServer.setAcceptCb((self, socket_) -> onAccept(socket_));
    }
    public Broker(InetSocketAddress addr) {
        this(jl95.net.Util.getSimpleServerSocket(addr));
    }

    private void                       onAccept       (Socket socket) {
        new Thread(() -> {
            var memberIdFuture   = new CompletableFuture<UUID>();
            var helloTypeFuture  = new CompletableFuture<Hello.Type>();
            var helloResponder   = Responder.fromIo(Ios.fromSocket(socket))
                    .adapted(SerdesDefaults.jsonFromBytes, SerdesDefaults.jsonToBytes)
                    .adapted(MessageDeserializer.get(HelloJsonSerdes::fromJson), (UUID id) -> SerdesDefaults.stringToJson.apply(id.toString()));
            var acceptedFuture = new CompletableFuture<Void>();
            helloResponder.respondOnce(hello -> {
                helloTypeFuture.complete(hello.body.type);
                memberIdFuture .complete(hello.memberId);
                uncheck(() -> acceptedFuture.get());
                return getBrokerId();
            }).get();
            var helloType = uncheck(() -> helloTypeFuture.get());
            var memberId  = uncheck(() -> memberIdFuture .get());
            Method2<UUID, Socket> register;
            switch (helloType) {
                case MEMBER_REQUESTS  -> register = this::registerMemberRequestsLink;
                case MEMBER_RESPONSES -> register = this::registerMemberResponsesLink;
                case BROKER_REQUESTS  -> register = this::registerBrokerRequestsLink;
                case BROKER_RESPONSES -> register = this::registerBrokerResponsesLink;
                default -> throw new AssertionError();
            }
            register.accept(memberId, socket);
            acceptedFuture.complete(null);
            if (helloResponder.isRunning()) {
                helloResponder.stop().get();
            }
        }).start();
    }
    private void                       resetConnection(UUID memberId) {
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
            for (var e: brokerResponsesMap.entrySet()) {
                var brokerId         = e.getKey  ();
                var brokerConnection = e.getValue();
                if (msg.stamps.contains(brokerId)) {
                    continue;
                }
                brokerConnection.addToQueue(brokerCbCallerSupplier.apply(msg));
            }
            return re;
        };
    }
    private RequestHandler<Close>      getCloseReqHandler(UUID entityId) { return msg -> !entityId.equals(msg.memberId); }
    private <S extends Subscription>
            RequestHandler<S>          getSubReqHandler  (UUID entityId) {
        return msg -> {
            setSubscription(msg.memberId, msg.body);
            notifyListenersUpdateSubscription(new UpdateSubscription(msg.memberId, msg.body));
            return true;
        };
    }
    private RequestHandler<Publication>getPubReqHandler  (UUID entityId) {
        return msg -> {
            var pub = msg.body;
            for (var idOfOther: subscriptionsMap.keySet()) {
                if (!getSubscription(idOfOther).accepts(pub.topicName)) continue;
                if (!memberResponsesMap.containsKey(idOfOther)) continue;
                memberResponsesMap.get(idOfOther).addToQueue(msg);
            }
            return true;
        };
    }
    private void                       linkBroker        (Socket requestsSocket,
                                                          Socket responsesSocket) {
        synchronized (brokerLinkSync) {
            var requestsIos  = CloseableIos.fromSocketLazy(requestsSocket);
            var responsesIos = CloseableIos.fromSocketLazy(responsesSocket);
            var requestsHelloRequester = Requester.fromIo(requestsIos)
                    .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                    .adaptedRequest (MessageSerializer.get(HelloJsonSerdes::toJson))
                    .adaptedResponse(json -> UUID.fromString(SerdesDefaults.stringFromJson.apply(json)));
            var responsesHelloRequester = Requester.fromIo(responsesIos)
                    .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                    .adaptedRequest (MessageSerializer.get(HelloJsonSerdes::toJson))
                    .adaptedResponse(json -> UUID.fromString(SerdesDefaults.stringFromJson.apply(json)));
            for (var t: I(
                tuple(Hello.Type.BROKER_REQUESTS,  requestsHelloRequester,  requestsSocket,  method(this::registerBrokerResponsesLink)),
                tuple(Hello.Type.BROKER_RESPONSES, responsesHelloRequester, responsesSocket, method(this::registerBrokerRequestsLink))
            )) {
                var helloMsg = new Message<Hello>();
                helloMsg.id       = UUID.randomUUID();
                helloMsg.body     = new Hello(t.a1);
                helloMsg.memberId = brokerId;
                var otherId = t.a2.apply(helloMsg).get();
                t.a4.accept(otherId, t.a3);
            }
        }
    }
    private void                       registerMemberRequestsLink (UUID memberId, Socket socket) {
        var connection = new MemberRequestsConnection(socket);
        memberRequestsMap.put(memberId, connection);
        connection.closeReqHandler    = decorated(getCloseReqHandler(memberId), msg -> cbs -> {});
        connection.subListReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubList (msg));
        connection.subRegexReqHandler = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubRegex(msg));
        connection.subAllReqHandler   = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubAll  (msg));
        connection.subNoneReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubNone (msg));
        connection.pubReqHandler      = decorated(getPubReqHandler  (memberId), msg -> cbs -> cbs.onPub     (msg));
        connection.startRespond().get();
    }
    private void                       registerMemberResponsesLink(UUID memberId, Socket socket) {
        var connection = new MemberResponsesConnection(socket);
        memberResponsesMap.put(memberId, connection);
        connection.startQueueLoop();
    }
    private void                       registerBrokerRequestsLink (UUID otherId, Socket socket) {
        synchronized (brokerLinkSync) {
            var connection = new BrokerRequestsConnection(socket);
            brokerRequestsMap.put(otherId, connection);
            connection.closeReqHandler    = decorated(getCloseReqHandler(otherId), msg -> cbs -> {});
            connection.subListReqHandler  = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubList (msg));
            connection.subRegexReqHandler = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubRegex(msg));
            connection.subAllReqHandler   = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubAll  (msg));
            connection.subNoneReqHandler  = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubNone (msg));
            connection.pubReqHandler      = decorated(getPubReqHandler  (otherId), msg -> cbs -> cbs.onPub     (msg));
            connection.startRespond().get();
        }
    }
    private void                       registerBrokerResponsesLink(UUID otherId, Socket socket) {
        synchronized (brokerLinkSync) {
            var connection = new BrokerResponsesConnection(socket);
            brokerResponsesMap.put(otherId, connection);
            connection.startQueueLoop();
        }
    }
    // listeners
    synchronized
    private void                       notifyListenersUpdateSubscription(UpdateSubscription update) {
        for (var listener: listenersOnUpdateSubscriptionMap.values()) {
            listener.accept(update);
        }
    }

    public final UVoidFuture startAccept     () {

        return netServer.start();
    }
    public final UVoidFuture              stopAccept      () {

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
        linkBroker(jl95.net.Util.getSocketByConnect(addr),
                   jl95.net.Util.getSocketByConnect(addr));
    }
    public final void                       resetConnections() {

        for (var clientId: memberRequestsMap.keySet()) {
            resetConnection(clientId);
        }
    }
    public final jl95.net.Server            getNetServer    () {

        return netServer;
    }
    public final void                       close           () {
        resetConnections();
        getNetServer().close();
    }
    // listeners
    public final AutoMapper<UUID, Method1<UpdateSubscription>> listenersOnUpdateSubscription(Method1<UpdateSubscription> listener) {
        return listenersOnUpdateSubscriptionAutoMapper;
    }
}

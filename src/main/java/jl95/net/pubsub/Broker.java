package jl95.net.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import jl95.net.io.CloseableIOStreamSupplier;
import jl95.net.io.IOStreamSupplier;
import jl95.net.rpc.BytesIOSRRequester;
import jl95.net.rpc.BytesIOSRResponder;
import jl95.util.*;
import jl95.net.pubsub.listen.UpdateSubscription;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.BrokerResponder;
import jl95.net.pubsub.util.BrokerRequester;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.MemberRequester;
import jl95.net.pubsub.util.MemberResponder;
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
    private final StrictMap<UUID, MemberResponder> memberRespondersMap = strict(new ConcurrentHashMap<>());
    private final StrictMap<UUID, MemberRequester> memberRequestersMap = strict(new ConcurrentHashMap<>());
    private final StrictMap<UUID, BrokerResponder> brokerRespondersMap = strict(new ConcurrentHashMap<>());
    private final StrictMap<UUID, BrokerRequester> brokerRequestersMap = strict(new ConcurrentHashMap<>());
    private final Object brokerLinkSync = new Object();
    private final StrictMap<UUID, Subscription> subscriptionsMap   = strict(new ConcurrentHashMap<>());
    // networking
    private final jl95.net.Server netServer;
    // listeners
    private final StrictMap <UUID, Method1<UpdateSubscription>> listenersOnUpdateSubscriptionMap        = strict(new ConcurrentHashMap<>());

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
            var helloResponder   = BytesIOSRResponder.of(IOStreamSupplier.fromSocket(socket))
                    .adapted(SerdesDefaults.jsonFromBytes, SerdesDefaults.jsonToBytes)
                    .adapted(MessageDeserializer.get(HelloJsonSerdes::fromJson), (UUID id) -> SerdesDefaults.stringToJson.apply(id.toString()));
            var acceptedFuture = new CompletableFuture<Void>();
            new Thread(() -> helloResponder.respondOnce(hello -> {
                helloTypeFuture.complete(hello.body.type);
                memberIdFuture .complete(hello.memberId);
                uncheck(() -> acceptedFuture.get());
                return getBrokerId();
            })).start();
            var helloType = uncheck(() -> helloTypeFuture.get());
            var memberId  = uncheck(() -> memberIdFuture .get());
            Method2<UUID, Socket> register;
            switch (helloType) {
                case MEMBER_REQUESTS  -> register = this::registerMemberResponder;
                case MEMBER_RESPONSES -> register = this::registerMemberRequester;
                case BROKER_REQUESTS  -> register = this::registerBrokerResponder;
                case BROKER_RESPONSES -> register = this::registerBrokerRequester;
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
        memberRespondersMap.get   (memberId).close();
        memberRespondersMap.remove(memberId);
        memberRequestersMap.get   (memberId).close();
        memberRequestersMap.remove(memberId);
    }
    private <T>
            RequestHandler<T>          decorated         (RequestHandler<T> handler, Function1<Method1<BrokerRequester.Callbacks>,
                                                          Message<T>> brokerCbCallerSupplier) {
        return msg -> {
            msg.stamps.add(getBrokerId());
            final Boolean re = handler.apply(msg);
            for (var e: brokerRequestersMap.entrySet()) {
                final UUID brokerId = e.getKey();
                final BrokerRequester brokerConnection = e.getValue();
                if (msg.stamps.contains(brokerId)) {
                    continue;
                }
                brokerConnection.addToQueue(brokerCbCallerSupplier.apply(msg));
            }
            return re;
        };
    }
    private RequestHandler<Close>      getCloseReqHandler(UUID entityId) {
        return msg -> !entityId.equals(msg.memberId);
    }
    private <S extends Subscription>
            RequestHandler<S>          getSubReqHandler  (UUID entityId) {
        return msg -> {
            setSubscription(msg.memberId, msg.body);
            notifyListenersOnUpdateSubscription(new UpdateSubscription(msg.memberId, msg.body));
            return true;
        };
    }
    private RequestHandler<Publication>getPubReqHandler  (UUID entityId) {
        return msg -> {
            var pub = msg.body;
            for (final UUID idOfOther: subscriptionsMap.keySet()) {
                if (!getSubscription(idOfOther).accepts(pub.topicName)) {
                    continue;
                }
                if (!memberRequestersMap.containsKey(idOfOther)) {
                    continue;
                }
                memberRequestersMap.get(idOfOther).sendPub(msg);
            }
            return true;
        };
    }
    private void                       linkBroker        (Socket requestsSocket,
                                                          Socket responsesSocket) {
        synchronized (brokerLinkSync) {
            final var requestsIos  = CloseableIOStreamSupplier.fromSocketLazy(requestsSocket);
            final var responsesIos = CloseableIOStreamSupplier.fromSocketLazy(responsesSocket);
            final var requestsHelloRequester = BytesIOSRRequester.of(requestsIos)
                    .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                    .adaptedRequest (MessageSerializer.get(HelloJsonSerdes::toJson))
                    .adaptedResponse(json -> UUID.fromString(SerdesDefaults.stringFromJson.apply(json)));
            final var responsesHelloRequester = BytesIOSRRequester.of(responsesIos)
                    .adapted(SerdesDefaults.jsonToBytes, SerdesDefaults.jsonFromBytes)
                    .adaptedRequest (MessageSerializer.get(HelloJsonSerdes::toJson))
                    .adaptedResponse(json -> UUID.fromString(SerdesDefaults.stringFromJson.apply(json)));
            for (final Tuple4<Hello.Type, jl95.net.rpc.IOSRRequester<Message<Hello>, UUID>, Socket, Method2<UUID, Socket>> t: I(
                tuple(Hello.Type.BROKER_REQUESTS,  requestsHelloRequester,  requestsSocket,  method(this::registerBrokerRequester)),
                tuple(Hello.Type.BROKER_RESPONSES, responsesHelloRequester, responsesSocket, method(this::registerBrokerResponder))
            )) {
                final var helloMsg = new Message<Hello>();
                helloMsg.id       = UUID.randomUUID();
                helloMsg.body     = new Hello(t.a1);
                helloMsg.memberId = brokerId;
                final var otherId = t.a2.apply(helloMsg).get();
                t.a4.accept(otherId, t.a3);
            }
        }
    }

    private void registerMemberResponder(UUID memberId, Socket socket) {
        var connection = new MemberResponder(socket);
        memberRespondersMap.put(memberId, connection);
        connection.closeReqHandler    = decorated(getCloseReqHandler(memberId), msg -> cbs -> {});
        connection.subListReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubList (msg));
        connection.subRegexReqHandler = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubRegex(msg));
        connection.subAllReqHandler   = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubAll  (msg));
        connection.subNoneReqHandler  = decorated(getSubReqHandler  (memberId), msg -> cbs -> cbs.onSubNone (msg));
        connection.pubReqHandler      = decorated(getPubReqHandler  (memberId), msg -> cbs -> cbs.onPub     (msg));
        connection.startRespond().get();
    }
    private void registerMemberRequester(UUID memberId, Socket socket) {
        var connection = new MemberRequester(socket);
        memberRequestersMap.put(memberId, connection);
        connection.startQueueLoop();
    }
    private void registerBrokerResponder(UUID otherId, Socket socket) {
        synchronized (brokerLinkSync) {
            var connection = new BrokerResponder(socket);
            brokerRespondersMap.put(otherId, connection);
            connection.closeReqHandler    = decorated(getCloseReqHandler(otherId), msg -> cbs -> {});
            connection.subListReqHandler  = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubList (msg));
            connection.subRegexReqHandler = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubRegex(msg));
            connection.subAllReqHandler   = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubAll  (msg));
            connection.subNoneReqHandler  = decorated(getSubReqHandler  (otherId), msg -> cbs -> cbs.onSubNone (msg));
            connection.pubReqHandler      = decorated(getPubReqHandler  (otherId), msg -> cbs -> cbs.onPub     (msg));
            connection.startRespond().get();
        }
    }
    private void registerBrokerRequester(UUID otherId, Socket socket) {
        synchronized (brokerLinkSync) {
            var connection = new BrokerRequester(socket);
            brokerRequestersMap.put(otherId, connection);
            connection.startQueueLoop();
        }
    }

    // listeners
    private synchronized void notifyListenersOnUpdateSubscription(UpdateSubscription update) {
        for (var listener: listenersOnUpdateSubscriptionMap.values()) {
            listener.accept(update);
        }
    }

    public final UVoidFuture                startAccept     () {

        return netServer.start();
    }
    public final UVoidFuture                stopAccept      () {

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

        return I.of(memberRespondersMap.keySet());
    }
    public final Iterable<InetSocketAddress>getAddresses    () {

        return I.of(memberRespondersMap.values())
                 .map(MemberResponder::getSocket)
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

        for (var clientId: memberRespondersMap.keySet()) {
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
}

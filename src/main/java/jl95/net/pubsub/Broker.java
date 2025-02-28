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
import jl95.net.io.Ios;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Hello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.RequestingConnection;
import jl95.net.pubsub.util.RespondingConnection;
import jl95.net.io.util.Util;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.protocol.HelloJsonSerdes;
import jl95.net.rpc.Responder;
import jl95.net.rpc.collections.ResponderAdaptersCollection;

public class Broker {

    private final UUID                                  brokerId         = UUID.randomUUID();
    private final StrictMap<UUID, RespondingConnection> respondingMap    = StrictMap.of(new ConcurrentHashMap<>());
    private final StrictMap<UUID, RequestingConnection> requestingMap    = StrictMap.of(new ConcurrentHashMap<>());
    private final StrictMap<UUID, Subscription>         subscriptionsMap = StrictMap.of(new ConcurrentHashMap<>());

    private final jl95.net.Server netServer;

    public Broker(ServerSocket      socket) {
        this.netServer = new jl95.net.Server(socket);
        this.netServer.setAcceptCb((self, socket_) -> onAccept(socket_));
    }
    public Broker(InetSocketAddress addr) {
        this(Util.getSimpleServerSocket(addr));
    }

    private void                                     onAccept            (Socket socket) {
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
                var connection = new RespondingConnection(socket);
                respondingMap.put(memberId, connection);
                connection.setCloseReqHandler            (getCloseReqHandler(connection, memberId));
                connection.setSubListReqHandler (decorate(getSubReqHandler  (connection, memberId)));
                connection.setSubRegexReqHandler(decorate(getSubReqHandler  (connection, memberId)));
                connection.setSubAllReqHandler  (decorate(getSubReqHandler  (connection, memberId)));
                connection.setSubNoneReqHandler (decorate(getSubReqHandler  (connection, memberId)));
                connection.setPubReqHandler     (decorate(getPubReqHandler  (connection, memberId)));
                connection.startRespond().await();
            }
            case MEMBER_RESPONSES -> {
                var connection = new RequestingConnection(socket);
                requestingMap.put(memberId, connection);
                connection.startPubQueue();
            }
            default -> throw new AssertionError();
        }
        acceptedFuture.complete(null);
        helloResponder.stop().await();
        assert !helloResponder.isRunning();
    }
    private void                                     closeConnection     (UUID UUID) {
        respondingMap.get   (UUID).close();
        respondingMap.remove(UUID);
        requestingMap.get   (UUID).close();
        requestingMap.remove(UUID);
    }
    private <T>
            Function1<Boolean, Message<T>>           decorate            (Function1<Boolean, Message<T>> handler) {
        return req -> {
            req.stamps.add(getBrokerId());
            return handler.apply(req);
        };
    }
    private Function1<Boolean, Message<Close>>       getCloseReqHandler  (RespondingConnection connection, UUID UUID) { return req -> {
        return false;
    }; }
    private <S extends Subscription>
            Function1<Boolean, Message<S>>           getSubReqHandler    (RespondingConnection connection, UUID UUID) {
        return req -> {
            setSubscription(UUID, req.body);
            return true;
        };
    }
    private Function1<Boolean, Message<Publication>> getPubReqHandler    (RespondingConnection connection, UUID UUID) {
        return req -> {
            var pub = req.body;
            for (var UUIDOfOther: subscriptionsMap.keySet()) {
                if (getSubscription(UUIDOfOther).accepts(pub.topicName)) {
                    if (requestingMap.containsKey(UUIDOfOther)) {
                        requestingMap.get(UUIDOfOther).pub(req);
                    }
                }
            }
            return true;
        };
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

        return I.of(respondingMap.keySet());
    }
    public final Iterable<InetSocketAddress> getAddressesLazy() {

        return I.of(respondingMap.values())
                 .map(RespondingConnection::getSocket)
                 .map(socket -> new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }
    public final Set<InetSocketAddress>     getAddresses    () {

            return I.of(getAddressesLazy()).toSet();
    }
    public final Subscription               getSubscription (UUID         UUID) {

        return subscriptionsMap.get(UUID);
    }
    public final void                       setSubscription (UUID         UUID,
                                                             Subscription subscription) {

        subscriptionsMap.put(UUID, subscription);
   }
    public final void                       closeConnections() {

        for (var clientId: respondingMap.keySet()) {
            closeConnection(clientId);
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

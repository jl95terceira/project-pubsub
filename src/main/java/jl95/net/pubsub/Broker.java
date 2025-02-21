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
import jl95.net.pubsub.protocol.MemberHello;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.Message;
import jl95.net.pubsub.util.RequestingConnection;
import jl95.net.pubsub.util.RespondingConnection;
import jl95.net.io.util.Util;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.protocol.MemberHelloJsonSerdes;
import jl95.net.rpc.Responder;
import jl95.net.rpc.collections.ResponderAdaptersCollection;

public class Broker {

    private final StrictMap<UUID, RespondingConnection> respondingMap = StrictMap.of(new ConcurrentHashMap<>());
    private final StrictMap<UUID, RequestingConnection> requestingMap = StrictMap.of(new ConcurrentHashMap<>());
    private final jl95.net.Server netServer;

    public Broker(ServerSocket      socket) {
        this.netServer = new jl95.net.Server(socket);
        this.netServer.setAcceptCb((self, socket_) -> onAccept(socket_));
    }
    public Broker(InetSocketAddress addr) {
        this(Util.getSimpleServerSocket(addr));
    }

    private InetSocketAddress                                   getMyAddress        () {
        var socket = getNetServer().getSocket();
        return new InetSocketAddress(socket.getInetAddress(), socket.getLocalPort());
    }
    private void                                                onAccept            (Socket socket) {
        var memberIdFuture  = new CompletableFuture<UUID>();
        var helloTypeFuture = new CompletableFuture<MemberHello.Type>();
        var memberHelloResponder = ResponderAdaptersCollection.asPostResponder(Responder.fromIo(Ios.fromSocket(socket))).adaptedRequest(
                MessageDeserializer.get(MemberHelloJsonSerdes::fromJson));
        var memberAcceptedFuture = new CompletableFuture<Void>();
        memberHelloResponder.respondOnce(hello -> {
            memberIdFuture .complete(hello.memberId);
            helloTypeFuture.complete(hello.body.type);
            uncheck(() -> memberAcceptedFuture.get());
            return null;
        }).await();
        var memberId   = uncheck(() -> memberIdFuture .get());
        var helloType  = uncheck(() -> helloTypeFuture.get());
        switch (helloType) {
            case REQUEST_FROM_BROKER -> {
                var connection = new RespondingConnection(socket);
                respondingMap.put(memberId, connection);
                connection.setCloseReqHandler          (getCloseReqHandler(connection, memberId));
                connection.setSubReqHandler  (decorate2(getSubReqHandler  (connection, memberId)));
                connection.setPubReqHandler  (decorate (getPubReqHandler  (connection, memberId)));
                connection.startRespond().await();
            }
            case RESPOND_TO_BROKER -> {
                var connection = new RequestingConnection(socket);
                requestingMap.put(memberId, connection);
                connection.startPubQueue();
            }
            default -> throw new AssertionError();
        }
        memberAcceptedFuture.complete(null);
        memberHelloResponder.stop().await();
        assert !memberHelloResponder.isRunning();
    }
    private void                                                closeConnection     (UUID memberId) {
        respondingMap.get   (memberId).close();
        respondingMap.remove(memberId);
        requestingMap.get   (memberId).close();
        requestingMap.remove(memberId);
    }
    private <T> Function1<Boolean, Message<T>>                  decorate            (Function1<Boolean, Message<T>> handler) {
        return req -> {
            req.stamps.add(getMyAddress());
            return handler.apply(req);
        };
    }
    private <T> Function1<Boolean, Message<? extends T>>        decorate2           (Function1<Boolean, Message<? extends T>> handler) {
        return req -> {
            req.stamps.add(getMyAddress());
            return handler.apply(req);
        };
    }
    private Function1<Boolean, Message<Close>>                  getCloseReqHandler  (RespondingConnection connection, UUID memberId) { return req -> {
        return false;
    }; }
    private Function1<Boolean, Message<? extends Subscription>> getSubReqHandler    (RespondingConnection connection, UUID memberId) {
        return req -> {
            requestingMap.get(memberId).setSubscription(req.body);
            return true;
        };
    }
    private Function1<Boolean, Message<Publication>>            getPubReqHandler    (RespondingConnection connection, UUID memberId) {
        return req -> {
            var pub = req.body;
            for (var other: requestingMap.values()) {
                if (other.getSubscription().accepts(pub.topicName)) {
                    other.pub(req);
                }
            }
            return true;
        };
    }

    public final Awaitable<Void>        startAccept     () {

        return netServer.start();
    }
    public final Awaitable<Void>        stopAccept      () {

        return netServer.stop();
    }
    public final Iterable<UUID>         getMemberIds    () {

        return I.of(respondingMap.keySet());
    }
    public final Iterable<InetSocketAddress> getAddressesLazy() {

        return I.of(respondingMap.values())
                 .map(RespondingConnection::getSocket)
                 .map(socket -> new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }
    public final Set<InetSocketAddress> getAddresses    () {

            return I.of(getAddressesLazy()).toSet();
    }
    public final Subscription           getSubscription (UUID memberId) {

        return requestingMap.get(memberId).getSubscription();
    }
    public final void                   setSubscription (UUID memberId,
                                                         Subscription subscription) {

        requestingMap.get(memberId).setSubscription(subscription);
    }
    public final void                   closeConnections() {

        for (var clientId: respondingMap.keySet()) {
            closeConnection(clientId);
        }
    }
    public final jl95.net.Server        getNetServer    () {

        return netServer;
    }
    public final void                   close           () {
        closeConnections();
        getNetServer().close();
    }
}

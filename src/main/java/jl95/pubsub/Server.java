package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.StrictMap;
import jl95.net.Receiver;
import jl95.net.util.Util;
import jl95.pubsub.protocol.Close;
import jl95.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.pubsub.util.ServerConnection;
import jl95.pubsub.util.MessageType;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.pubsub.protocol.Publication;
import jl95.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.pubsub.util.serdes.MessageSwitchedDeserializer;
import jl95.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;

public class Server {

    private final StrictMap<UUID, ServerConnection> connectionsMap = StrictMap.of(new ConcurrentHashMap<>());
    private final jl95.net.Server                   netServer;

    public Server(ServerSocket      socket) {
        this.netServer = new jl95.net.Server(socket);
        this.netServer.setAcceptCb((self, socket_) -> onAccept(socket_));
    }
    public Server(InetSocketAddress addr) {
        this(Util.getSimpleServerSocket(addr));
    }

    private InetSocketAddress                        getMyAddress       () {
        var socket = getNetServer().getSocket();
        return new InetSocketAddress(socket.getInetAddress(), socket.getLocalPort());
    }
    private void                                     onAccept           (Socket socket) {
        var connection = new ServerConnection(socket);
        // receive client ID and put in connections map
        var clientIdFuture = new CompletableFuture<String>();
        connection.stringReceiver.recvOnce(clientIdFuture::complete);
        var clientId = UUID.fromString(uncheck(() -> clientIdFuture.get()));
        connectionsMap.put(clientId, connection);
        // ...
        var switchingDeser = new MessageSwitchedDeserializer<Boolean>();
        switchingDeser.addCase(
            MessageType.REQ_CLOSE.serial,
            CloseJsonSerdes::fromJson,
            getCloseReqHandler(connection)
        );
        for (var t: I(
            tuple(MessageType.REQ_SUBSCRIPTION_BY_LIST .serial, function(SubscriptionByListJsonSerdes ::fromJson)),
            tuple(MessageType.REQ_SUBSCRIPTION_BY_REGEX.serial, function(SubscriptionByRegexJsonSerdes::fromJson)),
            tuple(MessageType.REQ_SUBSCRIPTION_TO_ALL  .serial, function(SubscriptionToAllJsonSerdes  ::fromJson)),
            tuple(MessageType.REQ_SUBSCRIPTION_TO_NONE .serial, function(SubscriptionToNoneJsonSerdes ::fromJson))
        )) {
            switchingDeser.addCase(
                t.a1,
                t.a2,
                decorated(getSubReqHandler(connection))
            );
        }
        switchingDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            decorated(getPubReqHandler(connection))
        );
        var recvOptions = new Receiver.RecvOptions.Editable<JsonValue>();
        recvOptions.afterStop = (receiver) -> {
            uncheck(connection.socket::close);
            connectionsMap.remove(clientId);
        };
        connection.jsonReceiver.recvWhile(switchingDeser, recvOptions);
        connection.startQueue();
    }
    private ServerConnection                         getConnection      (UUID clientId) {
        return connectionsMap.get(clientId);
    }
    private void                                     closeConnection    (UUID clientId) {
        var connection = connectionsMap.get(clientId);
        if (connection.isQueueRunning()) {
            connection.stopQueue().await();
        }
        uncheck(connection.socket::close);
        connectionsMap.remove(clientId);
    }
    private <T> Function1<Boolean, Message<T>>       decorated          (Function1<Boolean, Message<T>> handler) {
        return req -> {
            req.stamps.add(getMyAddress());
            return handler.apply(req);
        };
    }
    private Function1<Boolean, Message<Close>>       getCloseReqHandler (ServerConnection connection) { return req -> false; }
    private <S extends Subscription>
            Function1<Boolean, Message<S>>           getSubReqHandler   (ServerConnection connection) {
        return req -> {
            connection.subscription = req.body;
            return true;
        };
    }
    private Function1<Boolean, Message<Publication>> getPubReqHandler   (ServerConnection connection) {
        return req -> {
            var pub = req.body;
            for (var other: connectionsMap.values()) {
                if (other.subscription.accepts(pub.topicName)) {
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
    public final Iterable<InetSocketAddress> getAddressesLazy() {

        return I.of(connectionsMap.values())
                 .map(v -> v.socket)
                 .map(socket -> new InetSocketAddress(socket.getInetAddress(), socket.getPort()));
    }
    public final Set<InetSocketAddress> getAddresses    () {

            return I.of(getAddressesLazy()).toSet();
    }
    public final Subscription           getSubscription (UUID clientId) {

        return getConnection(clientId).subscription;
    }
    public final void                   setSubscription (UUID clientId,
                                                         Subscription subscription) {

        getConnection(clientId).subscription = subscription;
    }
    public final void                   closeConnections() {

        for (var clientId: connectionsMap.keySet()) {
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

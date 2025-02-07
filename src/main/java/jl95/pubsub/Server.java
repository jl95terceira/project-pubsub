package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.net.Receiver;
import jl95.net.util.Util;
import jl95.pubsub.protocol.requests.Close;
import jl95.pubsub.serdes.requests.SubscriptionByRegexJsonSerdes;
import jl95.pubsub.util.ServerConnection;
import jl95.pubsub.util.ServerConnectionKey;
import jl95.pubsub.util.MessageType;
import jl95.lang.I;
import jl95.lang.variadic.*;
import jl95.pubsub.protocol.Message;
import jl95.pubsub.protocol.requests.Publication;
import jl95.pubsub.serdes.PublicationJsonSerdes;
import jl95.pubsub.serdes.MessageSwitchedDeserializer;
import jl95.pubsub.serdes.requests.CloseJsonSerdes;
import jl95.pubsub.serdes.requests.SubscriptionByListJsonSerdes;
import jl95.pubsub.serdes.requests.SubscriptionToAllJsonSerdes;
import jl95.pubsub.serdes.requests.SubscriptionToNoneJsonSerdes;

public class Server {

    public interface Options {

        void onAcceptError  (Exception ex);
        void onAcceptTimeout();

        class Editable implements Server.Options {

            public Method1<Exception> acceptErrorCb   = (ex) -> System.out.printf("Error on accept connection: %s%n", ex);
            public Method0            acceptTimeoutCb = ()   -> {
            };

            @Override public void onAcceptError  (Exception ex) {
                acceptErrorCb.call(ex);
            }
            @Override public void onAcceptTimeout()             { acceptTimeoutCb.call(); }
        }
        static Options defaults() { return new Editable(); }
    }

    private final Map<ServerConnectionKey, ServerConnection>
                                    connectionsMap = new ConcurrentHashMap<>();
    private final jl95.net.Server   netServer;

    public Server(ServerSocket      socket,
                  Options           options) {
        this.netServer = new jl95.net.Server(socket, new jl95.net.Server.Options() {

            @Override public void         onAccept       (jl95.net.Server server, Socket    clientSocket) { Server.this.onAccept(clientSocket); }
            @Override public void         onAcceptError  (jl95.net.Server server, Exception ex) { options.onAcceptError(ex); }
            @Override public void         onAcceptTimeout(jl95.net.Server server) {
                options.onAcceptTimeout();
            }
        });
    }
    public Server(InetSocketAddress addr,
                  Options           options) {
        this(Util.getSimpleServerSocket(addr), options);
    }

    private void                                     onAccept          (Socket     socket) {
        var connection = new ServerConnection(socket);
        var key        = new ServerConnectionKey(socket);
        connectionsMap.put(key, connection);
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
                getSubReqHandler(connection)
            );
        }
        switchingDeser.addCase(
            MessageType.PUBLISH.serial,
            PublicationJsonSerdes::fromJson,
            getPubReqHandler(connection)
        );
        var recvOptions = new Receiver.RecvOptions.Editable<JsonValue>();
        recvOptions.afterStop = (receiver) -> {
            uncheck(connection.socket::close);
            connectionsMap.remove(key);
        };
        connection.jsonReceiver.recvWhile(switchingDeser, recvOptions);
        connection.startQueue();
    }
    private ServerConnection                         getConnection     (InetSocketAddress addr) {
        return connectionsMap.get(new ServerConnectionKey(addr));
    }
    private void                                     close             (ServerConnection connection) {
        if (connection.isQueueRunning()) {
            connection.stopQueue().await();
        }
        uncheck(connection.socket::close);
        connectionsMap.remove(new ServerConnectionKey(connection.socket));
    }
    private Function1<Boolean, Message<Close>>       getCloseReqHandler(ServerConnection connection) { return req -> false; }
    private <S extends Subscription>
            Function1<Boolean, Message<S>>           getSubReqHandler  (ServerConnection connection) {
        return req -> {
            connection.subscription = req.body;
            return true;
        };
    }
    private Function1<Boolean, Message<Publication>> getPubReqHandler  (ServerConnection connection) {
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

        return I.of(connectionsMap.keySet()).map(k -> k.inetSocketAddr);
    }
    public final Set<InetSocketAddress> getAddresses    () {

            return I.of(getAddressesLazy()).toSet();
    }
    public final Subscription           getSubscription (InetSocketAddress addr) {

        return getConnection(addr).subscription;
    }
    public final void                   setSubscription (InetSocketAddress addr,
                                                         Subscription subscription) {

        getConnection(addr).subscription = subscription;
    }
    public final void                   closeAll        () {

        for (var connection: connectionsMap.values()) {
            close(connection);
        }
    }
    public final jl95.net.Server        getNetServer    () {

        return netServer;
    }
}

package jl95.net.io.util;

import static jl95.lang.SuperPowers.uncheck;
import static jl95.lang.SuperPowers.unchecked;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

import jl95.lang.Awaitable;

public class Util {

    public static ServerSocket      getSimpleServerSocket       (InetSocketAddress addr,
                                                                 Integer           acceptTimeoutMs) {
        return uncheck(() -> {
            var socket = new ServerSocket();
            socket.bind(addr);
            socket.setSoTimeout(acceptTimeoutMs);
            return socket;
        });
    }
    public static ServerSocket      getSimpleServerSocket       (InetSocketAddress addr) {
        return getSimpleServerSocket(addr, Defaults.acceptTimeoutMs);
    }
    public static Awaitable<Socket> getSocketByAcceptFuture     (InetSocketAddress addr) {

        return uncheck(() -> {
            var serversock = new ServerSocket();
            serversock.bind(addr);
            CompletableFuture<Socket> socketFuture = new CompletableFuture<>();
            new Thread(unchecked(() -> {
                var sock = serversock.accept();
                serversock.close();
                socketFuture.complete(sock);
            })::accept).start();
            return Awaitable.of(socketFuture);
        });
    }
    public static Socket            getSocketByAccept           (InetSocketAddress addr) {

        return getSocketByAcceptFuture(addr).await();
    }
    public static Awaitable<Socket> getSocketByConnectFuture    (InetSocketAddress serverAddr) {
        var socket = new Socket();
        var future = new CompletableFuture<Socket>();
        new Thread(unchecked(() -> {
            socket.connect(serverAddr);
            future.complete(socket);
        })::accept).start();
        return Awaitable.of(future);
    }
    public static Socket            getSocketByConnect          (InetSocketAddress serverAddr) {
        return getSocketByConnectFuture(serverAddr).await();
    }
}

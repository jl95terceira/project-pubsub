package jl95.rpc.util;

import static jl95.lang.SuperPowers.uncheck;
import static jl95.lang.SuperPowers.unchecked;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jl95.net.Server;
import jl95.net.util.Defaults;

public class Util {

        public static CloseableIosSupplier getIoFromSocket(Socket            socket) {
            return new CloseableIosSupplier() {
                @Override public InputStream getInputStream() { return uncheck(socket::getInputStream); }
                @Override public OutputStream getOutputStream() { return uncheck(socket::getOutputStream); }
                @Override public void         close () {
                    if (!socket.isClosed()) {
                        uncheck(socket::close);
                    }
                }
            };
        }
        public static CloseableIosSupplier getIoAsClient  (InetSocketAddress addr) {
            var socket = new Socket();
            uncheck(() -> socket.connect(addr));
            return getIoFromSocket(socket);
        }
        public static CloseableIosSupplier getIoAsServer  (InetSocketAddress addr,
                                         Optional<Integer> clientConnectionTimeoutMs) {
            var serverOptions = new Server.Options.Editable();
            var clientSocketFuture = new CompletableFuture<Socket>();
            serverOptions.acceptCb = (self, socket) -> {
                clientSocketFuture.complete(socket);
            };
            var server = new Server(jl95.net.util.Util.getSimpleServerSocket(addr, Defaults.acceptTimeoutMs), serverOptions);
            server.start();
            var clientSocket = uncheck(() -> clientConnectionTimeoutMs.isPresent()
                                           ? clientSocketFuture.get(clientConnectionTimeoutMs.get(), TimeUnit.MILLISECONDS)
                                           : clientSocketFuture.get());
            server.stop().await(); // stop server right away - no need to accept more connections
            uncheck(server.getSocket()::close); // release bind address
            return getIoFromSocket(clientSocket);
        }
        public static CloseableIosSupplier getIoAsServer  (InetSocketAddress addr) { return getIoAsServer(addr, Optional.empty()); }
}

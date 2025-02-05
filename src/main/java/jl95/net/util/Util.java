package jl95.net.util;

import static jl95.lang.SuperPowers.uncheck;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class Util {

    public static ServerSocket getSimpleServerSocket(InetSocketAddress addr,
                                                     Integer           acceptTimeoutMs) {
        return uncheck(() -> {
            var socket = new ServerSocket();
            socket.bind(addr);
            socket.setSoTimeout(acceptTimeoutMs);
            return socket;
        });
    }
    public static ServerSocket getSimpleServerSocket(InetSocketAddress addr) {
        return getSimpleServerSocket(addr, Defaults.acceptTimeoutMs);
    }
}

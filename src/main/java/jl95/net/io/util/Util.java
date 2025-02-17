package jl95.net.io.util;

import static jl95.lang.SuperPowers.uncheck;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

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
    public static Socket       getConnectedSocket   (InetSocketAddress serverAddr) {
        var socket = new Socket();
        uncheck(() -> socket.connect(serverAddr));
        return socket;
    }
}

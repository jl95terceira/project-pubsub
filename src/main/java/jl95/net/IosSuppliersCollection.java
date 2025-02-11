package jl95.net;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.net.Socket;

public class IosSuppliersCollection {

    public static IosSupplier getSocketIos(Socket socket) {

        return IosSupplier.of(unchecked(socket::getInputStream), unchecked(socket::getOutputStream));
    }
}

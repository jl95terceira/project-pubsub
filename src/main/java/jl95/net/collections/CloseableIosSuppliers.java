package jl95.net;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;

import jl95.lang.variadic.*;

public class CloseableIosSuppliers {

    private static Function1<Method1<CloseableIosSupplier>, Socket> SOCKET_CLOSER = socket -> self -> {
            uncheck(socket::close);
        };

    public static CloseableIosSupplier getSocketIos(Socket socket) {

        return CloseableIosSupplier.of(uncheck(socket::getInputStream), uncheck(socket::getOutputStream), SOCKET_CLOSER.apply(socket));
    }
    public static CloseableIosSupplier getLazySocketIos(Socket socket) {

        return CloseableIosSupplier.of(unchecked(socket::getInputStream), unchecked(socket::getOutputStream), SOCKET_CLOSER.apply(socket));
    }
}

package jl95.net.collections;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;

import jl95.net.IosSupplier;

public class IosSuppliersCollection {

    public static IosSupplier getSocketIos(Socket socket) {

        return IosSupplier.of(unchecked(socket::getInputStream), unchecked(socket::getOutputStream));
    }
}

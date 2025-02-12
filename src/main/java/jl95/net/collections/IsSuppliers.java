package jl95.net.collections;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;

import jl95.net.IsSupplier;

public class IsSuppliers {

    static IsSupplier getSocketIs(Socket socket) {

        return IsSupplier.of(uncheck(socket::getInputStream));
    }
    static IsSupplier getLazySocketIs(Socket socket) {

        return IsSupplier.of(unchecked(socket::getInputStream));
    }
}

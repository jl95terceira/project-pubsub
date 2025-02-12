package jl95.net.collections;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;

import jl95.net.OsSupplier;

public class OsSuppliers {

    static OsSupplier getSockeOs(Socket socket) {

        return OsSupplier.of(uncheck(socket::getOutputStream));
    }
    static OsSupplier getLazySockeOs(Socket socket) {

        return OsSupplier.of(unchecked(socket::getOutputStream));
    }
}

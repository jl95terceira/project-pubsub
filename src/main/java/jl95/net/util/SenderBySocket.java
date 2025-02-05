package jl95.net.util;

import static jl95.lang.SuperPowers.*;

import java.io.OutputStream;

import jl95.lang.variadic.*;

public class SenderBySocket {

    public static <T> T get(java.net.Socket            socket,
                            Function1<T, OutputStream> constructor) {
        return constructor.call(uncheck(socket::getOutputStream));
    }
}

package jl95.net.io;

import static jl95.lang.SuperPowers.constant;
import static jl95.lang.SuperPowers.uncheck;
import static jl95.lang.SuperPowers.unchecked;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jl95.lang.variadic.Function0;
import jl95.net.io.util.InputStreams;
import jl95.net.io.util.OutputStreams;

public interface Os {

    OutputStream getOutputStream();

    static Os of            (Function0<OutputStream> out) {
        return out::apply;
    }
    static Os ofConstant    (OutputStream out) {
        return of(constant(out));
    }
    static Os fromSocket    (Socket socket) {

        return Os.ofConstant(uncheck(socket::getOutputStream));
    }
    static Os fromSocketLazy(Socket socket) {

        return Os.ofConstant(OutputStreams.getLazy(unchecked(socket::getOutputStream)));
    }
}

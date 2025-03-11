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

public interface Is {

    InputStream  getInputStream ();

    static Is of            (Function0<InputStream> in) {
        return in::apply;
    }
    static Is ofConstant    (InputStream in) {
        return of(constant(in));
    }
    static Is fromSocket    (Socket socket) {

        return Is.ofConstant(uncheck(socket::getInputStream));
    }
    static Is fromSocketLazy(Socket socket) {

        return Is.ofConstant(InputStreams.getLazy(unchecked(socket::getInputStream)));
    }
}

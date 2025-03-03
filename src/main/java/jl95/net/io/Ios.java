package jl95.net.io;

import static jl95.lang.SuperPowers.constant;
import static jl95.lang.SuperPowers.uncheck;
import static jl95.lang.SuperPowers.unchecked;

import java.io.*;
import java.net.Socket;

import jl95.lang.variadic.ExceptFunction1;
import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Function1;
import jl95.net.io.managed.ManagedIos;
import jl95.net.io.util.InputStreams;
import jl95.net.io.util.OutputStreams;

public interface Ios {

    InputStream  getInputStream ();
    OutputStream getOutputStream();

    static Ios of        (Function0<InputStream>  in,
                          Function0<OutputStream> out) {
        return new Ios() {
            @Override public InputStream  getInputStream () {
                return in .apply();
            }
            @Override public OutputStream getOutputStream() {
                return out.apply();
            }
        };
    }
    static Ios ofConstant(InputStream  in,
                          OutputStream out) {
        return of(constant(in), constant(out));
    }
    static Ios fromSocket    (Socket socket) {

        return Ios.ofConstant(uncheck(socket::getInputStream), uncheck(socket::getOutputStream));
    }
    static Ios fromSocketLazy(Socket socket) {

        return Ios.ofConstant(InputStreams.getLazy(unchecked(socket::getInputStream)), OutputStreams.getLazy(unchecked(socket::getOutputStream)));
    }
}

package jl95.net;

import static jl95.lang.SuperPowers.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;

public interface CloseableIos extends Ios {

    void close();

    static CloseableIos of(InputStream  is,
                           OutputStream os) { return of(is, os, self -> {}); }
    static CloseableIos of(InputStream  is,
                           OutputStream os,
                           Method1<CloseableIos> closer) {
        return new CloseableIos() {
            @Override public InputStream  getInputStream () {
                return is;
            }
            @Override public OutputStream getOutputStream() {
                return os;
            }
            @Override public void         close          () { closer.accept(this); }
        };
    }

    Function1<Method1<CloseableIos>, Socket> SOCKET_CLOSER = socket -> self -> {
            uncheck(socket::close);
        };

    static CloseableIos getSocketIos    (Socket socket) {

        var ios = Ios.getLazySocketIos(socket);
        return CloseableIos.of(ios.getInputStream(), ios.getOutputStream(), SOCKET_CLOSER.apply(socket));
    }
    static CloseableIos getLazySocketIos(Socket socket) {

        var ios = Ios.getLazySocketIos(socket);
        return CloseableIos.of(ios.getInputStream(), ios.getOutputStream(), SOCKET_CLOSER.apply(socket));
    }
}

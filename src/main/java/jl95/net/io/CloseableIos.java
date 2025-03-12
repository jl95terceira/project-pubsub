package jl95.net.io;

import static jl95.lang.SuperPowers.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;

public interface CloseableIos extends Ios, Closeable {

    static CloseableIos of(Ios ios) { return of(ios, self -> {}); }
    static CloseableIos of(InputStream  is,
                           OutputStream os) { return of(Ios.ofConstant(is, os)); }
    static CloseableIos of(Ios ios,
                           Method1<CloseableIos> closer) {
        return new CloseableIos() {
            @Override public InputStream  getInputStream () {
                return ios.getInputStream();
            }
            @Override public OutputStream getOutputStream() {
                return ios.getOutputStream();
            }
            @Override public void         close          () { closer.accept(this); }
        };
    }
    static CloseableIos of(InputStream  is,
                           OutputStream os,
                           Method1<CloseableIos> closer) {
        return of(Ios.ofConstant(is, os), closer);
    }

    Function1<Method1<CloseableIos>, Socket> SOCKET_CLOSER = socket -> self -> {
            uncheck(socket::close);
        };

    static CloseableIos fromSocket    (Socket socket) {

        var ios = Ios.fromSocketLazy(socket);
        return CloseableIos.of(ios.getInputStream(), ios.getOutputStream(), SOCKET_CLOSER.apply(socket));
    }
    static CloseableIos fromSocketLazy(Socket socket) {

        var ios = Ios.fromSocketLazy(socket);
        return CloseableIos.of(ios.getInputStream(), ios.getOutputStream(), SOCKET_CLOSER.apply(socket));
    }
}

package jl95.net;

import static jl95.lang.SuperPowers.unchecked;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jl95.lang.variadic.Function0;

@FunctionalInterface
public interface OsSupplier {

    OutputStream getOutputStream();

    static OsSupplier of(OutputStream            os) {
        return new OsSupplier() {
            @Override public OutputStream getOutputStream() {
                return os;
            }
        };
    }
    static OsSupplier of(Function0<OutputStream> osSupplier) {
        return new OsSupplier() {
            @Override public OutputStream getOutputStream() {
                return osSupplier.apply();
            }
        };
    }
    static OsSupplier of(Socket                  socket) {

        return OsSupplier.of(unchecked(socket::getOutputStream));
    }
}

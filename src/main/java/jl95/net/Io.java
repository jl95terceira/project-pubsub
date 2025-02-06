package jl95.net;

import static jl95.lang.SuperPowers.uncheck;

import java.io.*;
import java.net.Socket;

public interface Io {

    InputStream  getInputStream ();
    OutputStream getOutputStream();

    static Io of(InputStream is, OutputStream os) {
        return new Io() {
            @Override public InputStream getInputStream() {
                return is;
            }
            @Override public OutputStream getOutputStream() {
                return os;
            }
        };
    }
    static Io of(Socket socket) {
        return Io.of(uncheck(socket::getInputStream), uncheck(socket::getOutputStream));
    }
}

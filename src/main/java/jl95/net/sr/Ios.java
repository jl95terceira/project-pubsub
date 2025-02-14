package jl95.net.sr;

import static jl95.lang.SuperPowers.uncheck;
import static jl95.lang.SuperPowers.unchecked;

import java.io.*;
import java.net.Socket;

import jl95.net.sr.util.InputStreams;
import jl95.net.sr.util.OutputStreams;

public interface Ios {

    InputStream  getInputStream ();
    OutputStream getOutputStream();

    static Ios of(InputStream  in,
                  OutputStream out) {
        return new Ios() {
            @Override public InputStream getInputStream() {
                return in;
            }
            @Override public OutputStream getOutputStream() {
                return out;
            }
        };
    }
    static Ios fromSocket    (Socket socket) {

        return Ios.of(uncheck(socket::getInputStream), uncheck(socket::getOutputStream));
    }
    static Ios fromSocketLazy(Socket socket) {

        return Ios.of(InputStreams.getLazy(unchecked(socket::getInputStream)), OutputStreams.getLazy(unchecked(socket::getOutputStream)));
    }
}

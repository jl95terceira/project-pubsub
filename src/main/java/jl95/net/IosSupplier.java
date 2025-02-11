package jl95.net;

import static jl95.lang.SuperPowers.uncheck;
import static jl95.lang.SuperPowers.unchecked;

import java.io.*;
import java.net.Socket;

import jl95.lang.variadic.Function0;

public interface IosSupplier extends IsSupplier, OsSupplier {

    static IosSupplier of(InputStream             is,
                          OutputStream            os) { return of(() -> is, () -> os); }
    static IosSupplier of(Function0<InputStream>  isSupplier,
                          Function0<OutputStream> osSupplier) {
        return new IosSupplier() {
            @Override public InputStream  getInputStream () {
                return isSupplier.apply();
            }
            @Override public OutputStream getOutputStream() {
                return osSupplier.apply();
            }
        };
    }
}

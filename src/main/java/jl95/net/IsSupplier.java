package jl95.net;

import static jl95.lang.SuperPowers.unchecked;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jl95.lang.variadic.Function0;

@FunctionalInterface
public interface IsSupplier {

    InputStream  getInputStream ();

    static IsSupplier of(InputStream             is) {
        return new IsSupplier() {
            @Override public InputStream  getInputStream () {
                return is;
            }
        };
    }
    static IsSupplier of(Function0<InputStream>  isSupplier) {
        return new IsSupplier() {
            @Override public InputStream  getInputStream () {
                return isSupplier.apply();
            }
        };
    }
}

package jl95.net.x;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import jl95.lang.variadic.ExceptFunction1;
import jl95.lang.variadic.ExceptMethod1;

public interface ManagedIos {

    <T> T withInput (ExceptFunction1<T, IOException, InputStream> f);
    <T> T withOutput(ExceptFunction1<T, IOException, OutputStream> f);

    default void  withInput (ExceptMethod1<IOException, InputStream> f) {

        this.<Void>withInput(in -> {
            f.accept(in);
            return null;
        });
    }
    default void  withOutput(ExceptMethod1<IOException, OutputStream> f) {

        this.<Void>withOutput(out -> {
            f.accept(out);
            return null;
        });
    }
}

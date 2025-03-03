package jl95.net.io.managed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import jl95.lang.variadic.ExceptFunction1;
import jl95.lang.variadic.ExceptMethod1;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;
import jl95.net.io.Ios;

public interface ManagedIos {

    <T> T withInput (Function1<T, InputStream>  f);
    <T> T withOutput(Function1<T, OutputStream> f);

    default void  withInput (Method1<InputStream> f) {

        this.<Void>withInput(in -> {
            f.accept(in);
            return null;
        });
    }
    default void  withOutput(Method1<OutputStream> f) {

        this.<Void>withOutput(out -> {
            f.accept(out);
            return null;
        });
    }

    static ManagedIos of(Ios ios) { return new ManagedIos() {
        @Override
        public <T> T withInput(Function1<T, InputStream> f) {
            return f.apply(ios.getInputStream());
        }

        @Override
        public <T> T withOutput(Function1<T, OutputStream> f) {
            return f.apply(ios.getOutputStream());
        }
    }; }
}

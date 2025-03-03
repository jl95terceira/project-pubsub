package jl95.net.io.managed;

import java.io.InputStream;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;

public interface ManagedIs {

    <T> T withInput (Function1<T, InputStream>  f);

    default void  withInput (Method1<InputStream> f) {

        this.<Void>withInput(in -> {
            f.accept(in);
            return null;
        });
    }

    static ManagedIs of(InputStream is) { return new ManagedIs() {
        @Override
        public <T> T withInput(Function1<T, InputStream> f) {
            return f.apply(is);
        }
    }; }
}

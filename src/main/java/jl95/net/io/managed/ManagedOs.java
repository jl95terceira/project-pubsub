package jl95.net.io.managed;

import java.io.OutputStream;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;

public interface ManagedOs {

    <T> T withOutput(Function1<T, OutputStream> f);

    default void  withOutput(Method1<OutputStream> f) {

        this.<Void>withOutput(out -> {
            f.accept(out);
            return null;
        });
    }

    static ManagedOs of(OutputStream os) { return new ManagedOs() {
        @Override
        public <T> T withOutput(Function1<T, OutputStream> f) {
            return f.apply(os);
        }
    }; }
}

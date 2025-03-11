package jl95.net.io.managed;

import java.io.OutputStream;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method1;
import jl95.net.io.Os;

public interface ManagedOs extends Os {

    <T> T withOutput(Function1<T, OutputStream> f);

    default void withOutput(Method1<OutputStream> f) {

        this.<Void>withOutput(out -> {
            f.accept(out);
            return null;
        });
    }

    @Override default OutputStream getOutputStream() { return withOutput(os -> os); }

    static ManagedOs of(OutputStream os) { return new ManagedOs() {
        @Override
        public <T> T withOutput(Function1<T, OutputStream> f) {
            return f.apply(os);
        }
    }; }
}

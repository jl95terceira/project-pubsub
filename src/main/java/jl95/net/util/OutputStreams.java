package jl95.net.util;

import java.io.IOException;
import java.io.OutputStream;

import jl95.lang.variadic.Function0;

public class OutputStreams {

    public static OutputStream getLazy(Function0<OutputStream> outSupplier) {
        return new OutputStream() {

            @Override
            public void close() throws IOException { outSupplier.apply().close(); }

            @Override
            public void flush() throws IOException { outSupplier.apply().flush(); }

            @Override
            public void write(byte[] b) throws IOException { outSupplier.apply().write(b); }

            @Override
            public void write(byte[] b, int off, int len) throws IOException { outSupplier.apply().write(b, off, len); }

            @Override
            public void write(int b) throws IOException {
                outSupplier.apply().write(b);
            }
        };
    }
}

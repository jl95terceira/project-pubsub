package jl95.net.io.util;

import java.io.IOException;
import java.io.OutputStream;

import jl95.lang.variadic.Function0;

public class OutputStreams {

    public static OutputStream getLazy(Function0<OutputStream> outSupplier) {
        return new OutputStream() {

            private OutputStream getOutputStream() {return outSupplier.apply();}

            @Override
            public void close() throws IOException { getOutputStream().close(); }

            @Override
            public void flush() throws IOException { getOutputStream().flush(); }

            @Override
            public void write(byte[] b) throws IOException { getOutputStream().write(b); }

            @Override
            public void write(byte[] b, int off, int len) throws IOException { getOutputStream().write(b, off, len); }

            @Override
            public void write(int b) throws IOException {
                getOutputStream().write(b);
            }
        };
    }
}

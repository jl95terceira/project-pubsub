package jl95.net.util;

import java.io.IOException;
import java.io.InputStream;

import jl95.lang.variadic.Function0;

public class InputStreams {

    public static InputStream getLazy(Function0<InputStream> inSupplier) {
        return new InputStream() {

            @Override
            public int available() throws IOException { return inSupplier.apply().available(); }

            @Override
            public void close() throws IOException { inSupplier.apply().close(); }

            @Override
            public void mark(int readlimit) { inSupplier.apply().mark(readlimit); }

            @Override
            public boolean markSupported() { return inSupplier.apply().markSupported(); }

            @Override
            public int read() throws IOException { return inSupplier.apply().read(); }

            @Override
            public int read(byte[] b) throws IOException { return inSupplier.apply().read(b); }

            @Override
            public int read(byte[] b, int off, int len) throws IOException { return inSupplier.apply().read(b, off, len); }

            @Override
            public void reset() throws IOException { inSupplier.apply().reset(); }

            @Override
            public long skip(long n) throws IOException { return inSupplier.apply().skip(n); }
        };
    }
}

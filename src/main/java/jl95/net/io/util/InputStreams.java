package jl95.net.io.util;

import java.io.IOException;
import java.io.InputStream;

import jl95.lang.variadic.Function0;

public class InputStreams {

    public static InputStream getLazy(Function0<InputStream> inSupplier) {
        return new InputStream() {

            private InputStream getInputStream() { return inSupplier.apply(); }

            @Override
            public int available() throws IOException { return getInputStream().available(); }

            @Override
            public void close() throws IOException { getInputStream().close(); }

            @Override
            public void mark(int readLimit) { getInputStream().mark(readLimit); }

            @Override
            public boolean markSupported() { return getInputStream().markSupported(); }

            @Override
            public int read() throws IOException { return getInputStream().read(); }

            @Override
            public int read(byte[] b) throws IOException { return getInputStream().read(b); }

            @Override
            public int read(byte[] b, int off, int len) throws IOException { return getInputStream().read(b, off, len); }

            @Override
            public void reset() throws IOException { getInputStream().reset(); }

            @Override
            public long skip(long n) throws IOException { return getInputStream().skip(n); }
        };
    }
}

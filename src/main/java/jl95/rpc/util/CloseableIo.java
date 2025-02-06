package jl95.rpc.util;

import java.io.Closeable;
import jl95.net.Io;

public interface CloseableIo extends Io {

    public void close();
}

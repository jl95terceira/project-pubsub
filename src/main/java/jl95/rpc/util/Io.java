package jl95.rpc.util;

import java.io.InputStream;
import java.io.OutputStream;

public interface Io {

    InputStream  input ();
    OutputStream output();
    void         close ();
}

package jl95.rpc;

import java.io.InputStream;
import java.io.OutputStream;

import jl95.rpc.util.SerdesDefaults;

public class StringResponder {

    public static Responder<String, String> get(InputStream input, OutputStream output) {

        return BytesResponder.get(input, output).adapted(
            SerdesDefaults.stringFromBytes,
            SerdesDefaults.stringToBytes
        );
    }
}

package jl95.rpc;

import java.io.InputStream;
import java.io.OutputStream;

import jl95.rpc.util.SerdesDefaults;

public class StringRequester {

    public static Requester<String, String> get(OutputStream output, InputStream input, GenericRequester.Options options) {

        return BytesRequester.get(output, input, options).adapted(
            SerdesDefaults.stringToBytes,
            SerdesDefaults.stringFromBytes
        );
    }
}

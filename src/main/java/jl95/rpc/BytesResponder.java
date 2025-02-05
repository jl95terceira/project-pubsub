package jl95.rpc;

import java.io.InputStream;
import java.io.OutputStream;

public class BytesResponder {

    public static Responder<byte[], byte[]> get(InputStream input, OutputStream output) {

        return new GenericResponder<>(input, output) {

            @Override protected byte[] readRequest  (byte[] serial) { return serial; }
            @Override protected byte[] writeResponse(byte[] object) {
                return object;
            }
        };
    }
}

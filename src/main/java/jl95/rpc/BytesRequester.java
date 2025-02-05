package jl95.rpc;

import java.io.InputStream;
import java.io.OutputStream;

public class BytesRequester {

    public static Requester<byte[], byte[]> get(OutputStream      output,
                                                InputStream       input,
                                                GenericRequester.Options options) {

        return new GenericRequester<>(output, input, options) {

            @Override protected byte[] writeRequest(byte[] object) {
                return object;
            }
            @Override protected byte[] readResponse(byte[] serial) { return serial; }
        };
    }
}

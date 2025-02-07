package jl95.rpc.impl;

import jl95.net.Io;
import jl95.rpc.Requester;
import jl95.rpc.RequesterIf;

public class BytesRequesterFactory {

    public static RequesterIf<byte[], byte[]> get(Io io, Requester.SendOptions options) {

        return new Requester<>(io) {

            @Override protected byte[] writeRequest(byte[] object) {
                return object;
            }
            @Override protected byte[] readResponse(byte[] serial) { return serial; }
        };
    }
}

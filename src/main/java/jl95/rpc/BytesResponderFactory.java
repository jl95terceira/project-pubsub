package jl95.rpc;

import jl95.net.Io;

public class BytesResponderFactory extends Responder<byte[], byte[]> {

    public BytesResponderFactory(Io io) {super(io);}

    @Override protected byte[] readRequest  (byte[] serial) { return serial; }
    @Override protected byte[] writeResponse(byte[] object) {
                return object;
            }
}

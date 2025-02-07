package jl95.rpc.impl;

import jl95.net.Io;
import jl95.rpc.Responder;

public class BytesResponderFactory extends Responder<byte[], byte[]> {

    public BytesResponderFactory(Io io) {super(io);}

    @Override protected byte[] readRequest  (byte[] serial) { return serial; }
    @Override protected byte[] writeResponse(byte[] object) {
                return object;
            }
}

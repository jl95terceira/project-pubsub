package jl95.net;

import java.io.OutputStream;

import jl95.lang.variadic.*;

public class BytesSender extends Sender<byte[]> {

    public BytesSender(OutputStream out) { super(out); }

    @Override protected byte[] toBytes  (byte[] outgoing) { return outgoing; }
}

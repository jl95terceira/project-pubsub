package jl95.net;

import static jl95.lang.SuperPowers.constant;

import java.io.OutputStream;

import jl95.lang.variadic.*;

public class Sender implements SenderIf<byte[]> {

    public static class SendException          extends RuntimeException {
        public SendException(Exception ex) {super(ex);}
    }

    public static Sender of(OutputStream os) {return new Sender(os);}

    private final OutputStream os;

    private Sender(OutputStream os) {
        this.os = os;
    }

    @Override
    synchronized public final void send(byte[] outgoing) {
        var size            = outgoing.length;
        var sizeAsBytes     = java.math.BigInteger.valueOf(size).toByteArray();
        try {
            os.write(sizeAsBytes.length);
            os.write(sizeAsBytes);
            os.write(outgoing);
        }
        catch (Exception ex) {
            throw new SendException(ex);
        }
    }

    @Override
    public final OutputStream getOutputStream() { return os; }
}
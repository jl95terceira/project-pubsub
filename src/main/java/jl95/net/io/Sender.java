package jl95.net.io;

import static jl95.lang.SuperPowers.*;

import java.io.OutputStream;

import jl95.net.io.managed.ManagedOs;

public class Sender implements SenderIf<byte[]> {

    public static class SendException          extends RuntimeException {
        public SendException(Exception ex) {super(ex);}
    }

    public static Sender of(ManagedOs    os) {return new Sender(os);}
    public static Sender of(OutputStream os) {return new Sender(ManagedOs.of(os));}

    private final ManagedOs mos;

    private Sender(ManagedOs mos) {

        this.mos = mos;
        flushOutputStream();
    }

    public final void flushOutputStream() {
        mos.withOutput(os -> {});
    }

    @Override
    synchronized public final void send(byte[] outgoing) {
        var size            = outgoing.length;
        var sizeAsBytes     = java.math.BigInteger.valueOf(size).toByteArray();
        mos.withOutput(os -> { uncheck(() -> {
            try {
                os.write(sizeAsBytes.length);
                os.write(sizeAsBytes);
                os.write(outgoing);
            }
            catch (Exception ex) {
                throw new SendException(ex);
            }
        }); });
    }

    @Override
    public final OutputStream getOutputStream() { return mos.getOutputStream(); }
}
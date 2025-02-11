package jl95.net;

import java.io.OutputStream;

import jl95.lang.variadic.*;

public abstract class Sender<T> {

    public static class SerializationException extends RuntimeException {
        public SerializationException(Exception ex) {super(ex);}
    }
    public static class SendException          extends RuntimeException {
        public SendException(Exception ex) {super(ex);}
    }

    private final OutputStream output;

    protected abstract byte[] toBytes(T outgoing);

    public Sender(OutputStream output) {
        this.output = output;
    }

    public final void         send           (T                outgoing) {
        byte[] outgoingAsBytes;
        try {
            outgoingAsBytes = toBytes(outgoing);
        }
        catch (Exception ex) {
            throw new SerializationException(ex);
        }
        var size            = outgoingAsBytes.length;
        var sizeAsBytes     = java.math.BigInteger.valueOf(size).toByteArray();
        try {
            output.write(sizeAsBytes.length);
            output.write(sizeAsBytes);
            output.write(outgoingAsBytes);
        }
        catch (Exception ex) {
            throw new SendException(ex);
        }
    }
    public final OutputStream getOutputStream() { return output; }
    public final <T2> Sender<T2> adapted(Function1<T, T2> adapterFunction) {

        return new Sender<>(output) {

            @Override protected byte[] toBytes(T2 incoming) {
                return Sender.this.toBytes(adapterFunction.call(incoming));
            }
        };
    }
}
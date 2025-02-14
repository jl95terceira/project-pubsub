package jl95.net;

import static jl95.lang.SuperPowers.sleep;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;

public class Sr implements SrIf<byte[], byte[]> {

    public static class SendException             extends RuntimeException {
        public SendException(Exception ex) {super(ex);}
    }
    public static class AlreadyReceivingException extends RuntimeException {}
    public static class NotYetReceivingException  extends RuntimeException {}

    public static Sr of(Ios ios) {
        return new Sr(ios);
    }

    private final Ios                     ios;
    private       Boolean                 isReceiving = false;
    private       Boolean                 toStop      = false;
    private       CompletableFuture<Void> startFuture;
    private       CompletableFuture<Void> stopFuture;

    private Sr(Ios ios) {
        this.ios = ios;
    }

    @Override
    synchronized public final void send(byte[] outgoing) {
        var size            = outgoing.length;
        var sizeAsBytes     = java.math.BigInteger.valueOf(size).toByteArray();
        try {
            var os = getOutputStream();
            os.write(sizeAsBytes.length);
            os.write(sizeAsBytes);
            os.write(outgoing);
        }
        catch (Exception ex) {
            throw new Sender.SendException(ex);
        }
    }
    @Override
    synchronized public final Awaitable<Void> recvWhile(Function1<Boolean, byte[]> incomingCbToContinue,
                                                        RecvOptions options) {
        if (isReceiving) {
            throw new AlreadyReceivingException();
        }
        toStop      = false;
        startFuture = new CompletableFuture<>();
        stopFuture  = new CompletableFuture<>();
        isReceiving = true;
        new Thread(() -> {
            startFuture.complete(null);
            while (!toStop) {
                byte[] incoming;
                try {
                    var in = getInputStream();
                    try {
                        if (in.available() == 0) {
                            options.onInputTimeout();
                            sleep(options.inputRetryTimeoutMs());
                            continue;
                        }
                        var sizeSize        = in.read();
                        if (sizeSize == -1) {
                            options.onInputTimeout();
                            sleep(options.inputRetryTimeoutMs());
                            continue;
                        }
                        var sizeAsBytes     = new byte[sizeSize];
                        in.read(sizeAsBytes, 0, sizeSize);
                        var size            = new java.math.BigInteger(sizeAsBytes).intValue();
                        incoming            = new byte[size];
                        in.read(incoming, 0, size);
                    }
                    catch (IOException ex) {
                        options.onIoException(ex);
                        break;
                    }
                    catch (Exception   ex) {
                        options.onProtocolException(ex);
                        break;
                    }
                    try {
                        var toContinue = incomingCbToContinue.apply(incoming);
                        if (!toContinue) {
                            toStop = true;
                        }
                    }
                    catch (Exception ex) {
                        options.onHandlingException(ex);
                        continue;
                    }
                }
                catch (Exception ex) {
                    System.out.println("UNHANDLED FOLLOW-UP EXCEPTION - stop recv");
                    ex.printStackTrace();
                    break;
                }
            }
            isReceiving = false;
            stopFuture.complete(null);
            options.afterStop();
        }).start();
        return Awaitable.of(startFuture);
    }
    @Override
    synchronized public final Awaitable<Void> recvStop() {

        if (!isReceiving) {
            throw new NotYetReceivingException();
        }
        toStop = true; // to be checked in loop, after which the future above will be completed
        return Awaitable.of(stopFuture);
    }
    @Override
    public final Boolean isReceiving() {
        return isReceiving;
    }
    @Override
    public final OutputStream getOutputStream() { return ios.getOutputStream(); }
    @Override
    public final InputStream getInputStream() { return ios.getInputStream(); }
}

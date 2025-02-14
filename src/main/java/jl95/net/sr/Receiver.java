package jl95.net.sr;

import static jl95.lang.SuperPowers.constant;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.uncheck;

import jl95.lang.*;
import jl95.lang.variadic.*;

public class Receiver implements ReceiverIf<byte[]> {

    public static class AlreadyReceivingException extends RuntimeException {}
    public static class NotYetReceivingException  extends RuntimeException {}

    public static Receiver of(InputStream is) {
        return new Receiver(is);
    }

    private final InputStream             in;
    private       Boolean                 isReceiving = false;
    private       Boolean                 toStop      = false;
    private       CompletableFuture<Void> startFuture;
    private       CompletableFuture<Void> stopFuture;

    private Receiver(InputStream is) {
        this.in = is;
    }

    @Override
    synchronized public final Awaitable<Void> recvWhile    (Function1<Boolean, byte[]> incomingCbToContinue,
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
    synchronized public final Awaitable<Void> recvStop     () {

        if (!isReceiving) {
            throw new NotYetReceivingException();
        }
        toStop = true; // to be checked in loop, after which the future above will be completed
        return Awaitable.of(stopFuture);
    }

    @Override
    public final Boolean           isReceiving   () {
        return isReceiving;
    }
    @Override
    public final InputStream       getInputStream() { return in; }
}

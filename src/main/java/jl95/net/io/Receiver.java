package jl95.net.io;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static jl95.lang.SuperPowers.self;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.uncheck;

import jl95.lang.*;
import jl95.lang.variadic.*;
import jl95.net.io.managed.ManagedIs;

public class Receiver implements ReceiverIf<byte[]> {

    public static class AlreadyReceivingException extends RuntimeException {}
    public static class NotYetReceivingException  extends RuntimeException {}

    public static Receiver of(ManagedIs   is) {
        return new Receiver(is);
    }
    public static Receiver of(InputStream is) {
        return new Receiver(ManagedIs.of(is));
    }

    private final    ManagedIs               mis;
    private final    ThreadPoolExecutor      pool = new ScheduledThreadPoolExecutor(1);
    private volatile Boolean                 isReceiving = false;
    private volatile Boolean                 toStop      = false;
    private          CompletableFuture<Void> startFuture;
    private          CompletableFuture<Void> stopFuture;

    private Receiver(ManagedIs is) {
        this.mis = is;
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
        pool.execute(() -> {
            startFuture.complete(null);
            while (!toStop) {
                var incoming = new Ref<byte[]>();
                try {
                    try {
                        var continueLoop = mis.withInput(is -> { return uncheck(() -> {
                            if (is.available() == 0) {
                                options.onInputTimeout();
                                sleep(options.inputRetryTimeoutMs());
                                return true;
                            }
                            var sizeSize        = is.read();
                            if (sizeSize == -1) {
                                options.onInputTimeout();
                                sleep(options.inputRetryTimeoutMs());
                                return true;
                            }
                            var sizeAsBytes     = new byte[sizeSize];
                            is.read(sizeAsBytes, 0, sizeSize);
                            var size            = new java.math.BigInteger(sizeAsBytes).intValue();
                            incoming.value      = new byte[size];
                            is.read(incoming.value, 0, size);
                            return false;
                        }); });
                        if (continueLoop) {
                            continue;
                        }
                    }
                    catch (Exception   ex) {
                        options.onInputException(ex);
                        break;
                    }
                    try {
                        var toContinue = incomingCbToContinue.apply(incoming.value);
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
                    System.out.println("Receiver: UNHANDLED FOLLOW-UP EXCEPTION - stop recv");
                    ex.printStackTrace();
                    break;
                }
            }
            isReceiving = false;
            stopFuture.complete(null);
            options.afterStop();
        });
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
    public final InputStream       getInputStream() { return mis.getInputStream(); }
  }

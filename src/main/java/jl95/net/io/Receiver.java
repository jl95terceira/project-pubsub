package jl95.net.io;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static jl95.lang.SuperPowers.function;
import static jl95.lang.SuperPowers.self;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.uncheck;

import jl95.lang.*;
import jl95.lang.variadic.*;
import jl95.net.io.managed.ManagedIs;

public class Receiver implements ReceiverIf<byte[]> {

    public static class AlreadyReceivingException extends RuntimeException {}
    public static class NotReceivingException extends RuntimeException {}

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

    private Receiver(ManagedIs mis) {

        this.mis = mis;
        flushInputStream();
    }

    private Awaitable<Void> recvStopUnchecked() {
        toStop = true; // to be checked in loop, after which the future above will be completed
        return Awaitable.of(stopFuture);
    }

    public final void flushInputStream() {
        mis.withInput(is -> {});
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
            var timeouts     = new Ref<>(0);
            var timeoutT0    = new Ref<>(Instant.now());
            while (!toStop) {
                var incoming = new Ref<byte[]>();
                try {
                    try {
                        var continueLoop = mis.withInput(is -> { return uncheck(() -> {
                            if (is.available() == 0) {
                                timeouts.set(v -> v + 1);
                                options.onInputTimeout(new TimeoutInfo(timeouts.get(), Duration.between(timeoutT0.get(), Instant.now())));
                                sleep(options.inputRetryTimeoutMs());
                                return true;
                            }
                            timeouts .set(0);
                            timeoutT0.set(Instant.now());
                            var sizeSize = is.read();
                            var sizeAsBytes = new byte[sizeSize];
                            is.read(sizeAsBytes, 0, sizeSize);
                            var size       = new java.math.BigInteger(sizeAsBytes).intValue();
                            incoming.set(new byte[size]);
                            is.read(incoming.get(), 0, size);
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
                        var toContinue = incomingCbToContinue.apply(incoming.get());
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
            throw new NotReceivingException();
        }
        return recvStopUnchecked();
    }

    @Override
    public final Boolean           isReceiving   () {
        return isReceiving;
    }
    @Override
    public final InputStream       getInputStream() { return mis.getInputStream(); }
  }

package jl95.net;

import static jl95.lang.SuperPowers.constant;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.*;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.uncheck;

import jl95.lang.*;
import jl95.lang.variadic.*;

public abstract class Receiver<T> {

    public interface RecvOptions<T> {

        void    afterStop          (Receiver<T> self);
        void    onException        (Receiver<T> self, Exception   ex);
        void    onIoException      (Receiver<T> self, IOException ex);
        void    onProtocolException(Receiver<T> self, Exception   ex);
        void    onInputTimeout     (Receiver<T> self);
        Integer inputRetryTimeoutMs();

        class Editable<T> implements RecvOptions<T> {

            public Method1<Receiver<T>>              afterStop           = (self) -> {};
            public Method2<Receiver<T>, Exception>   excHandler          = (self, ex) -> System.out.println(format("Error while handling incoming: %s", ex));
            public Method2<Receiver<T>, IOException> ioExcHandler        = (self, ex) -> System.out.println(format("Error while reading incoming: %s", ex));
            public Method2<Receiver<T>, Exception>   protocolExcHandler  = (self, ex) -> System.out.println(format("Error while deserializing incoming: %s", ex));
            public Method1<Receiver<T>>              inputTimeoutHandler = (self) ->  {};
            public Function0<Integer>                inputRetryTimeoutMs = constant(50);

            @Override public void afterStop          (Receiver<T> self) { afterStop.call(self); }
            @Override public void onException        (Receiver<T> self, Exception   ex) { excHandler        .call(self, ex); }
            @Override public void onIoException      (Receiver<T> self, IOException ex) { ioExcHandler      .call(self, ex); }
            @Override public void onProtocolException(Receiver<T> self, Exception   ex) { protocolExcHandler.call(self, ex); }
            @Override public void onInputTimeout     (Receiver<T> self) { inputTimeoutHandler.accept(self); }
            @Override public Integer inputRetryTimeoutMs() { return inputRetryTimeoutMs.apply(); }
        }
        static <T> RecvOptions<T> defaults() {
            return new Editable<>();
        }
    }

    public static class AlreadyReceivingException extends RuntimeException {}
    public static class NotYetReceivingException  extends RuntimeException {}

    private final InputStream             input;
    private       Boolean                 isReceiving = false;
    private       Boolean                 toStop      = false;
    private       CompletableFuture<Void> startFuture;
    private       CompletableFuture<Void> stopFuture;

    protected abstract T fromBytes(byte[] incoming);

    public Receiver(InputStream input) {
        this.input = input;
    }

    synchronized public final Awaitable<Void> recvWhile    (Function1<Boolean, T> incomingCbToContinue,
                                                            RecvOptions<T>        options) {
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
                byte[] incomingAsBytes;
                try {
                    try {
                        if (input.available() == 0) {
                            options.onInputTimeout(this);
                            sleep(options.inputRetryTimeoutMs());
                            continue;
                        }
                        var sizeSize        = input.read();
                        if (sizeSize == -1) {
                            options.onInputTimeout(this);
                            sleep(options.inputRetryTimeoutMs());
                            continue;
                        }
                        var sizeAsBytes     = new byte[sizeSize];
                        input.read(sizeAsBytes, 0, sizeSize);
                        var size            = new java.math.BigInteger(sizeAsBytes).intValue();
                        incomingAsBytes     = new byte[size];
                        input.read(incomingAsBytes, 0, size);
                    }
                    catch (IOException ex) {
                        options.onIoException(this, ex);
                        break;
                    }
                    catch (Exception   ex) {
                        options.onProtocolException(this, ex);
                        break;
                    }
                    try {
                        var toContinue = incomingCbToContinue.apply(fromBytes(incomingAsBytes));
                        if (!toContinue) {
                            toStop = true;
                        }
                    }
                    catch (Exception ex) {
                        options.onException(this, ex);
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
            options.afterStop(this);
        }).start();
        return Awaitable.of(startFuture);
    }
    synchronized public final Awaitable<Void> recvWhile    (Function1<Boolean, T> incomingCbToContinue) {

        return recvWhile(incomingCbToContinue, RecvOptions.defaults());
    }
    synchronized public final Awaitable<Void> recv         (Method1<T>            incomingCb,
                                                            RecvOptions<T>        options) {
        return recvWhile((T incoming) -> {
            incomingCb.call(incoming);
            return true;
        }, options);
    }
    synchronized public final Awaitable<Void> recv         (Method1<T>            incomingCb) {

        return recv(incomingCb, RecvOptions.defaults());
    }
    synchronized public final Awaitable<Void> recvStop     () {

        if (!isReceiving) {
            throw new NotYetReceivingException();
        }
        toStop = true; // to be checked in loop, after which the future above will be completed
        return Awaitable.of(stopFuture);
    }

    public final Boolean           isReceiving   () {
        return isReceiving;
    }
    public final InputStream       getInputStream() { return input; }
    public final <T2> Receiver<T2> extend        (Function1<T2, T> adapterFunction) {

        return new Receiver<>(input) {

            @Override protected T2 fromBytes(byte[] incoming) {
                return adapterFunction.call(Receiver.this.fromBytes(incoming));
            }
        };
    }
}

package jl95.net.x;

import static jl95.lang.SuperPowers.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import jl95.lang.variadic.*;
import jl95.net.*;
import jl95.net.util.Util;
import jl95.net.CloseableIosSupplier;

public class AutoReconnectingClientIos implements IosSupplier {

    public interface    Options {

        void    onIoException     (AutoReconnectingClientIos self, Exception exc);
        Integer reconnectTimeoutMs();
        Boolean toRetryReconnect  (Integer retriesSoFar);

        class Editable implements Options {

            public Method2<AutoReconnectingClientIos, Exception> reconnectExcHandler = (self, exc)    -> { exc.printStackTrace(); };
            public Function0<Integer>                            reconnectTimeoutMs  = () -> 3000;
            public Function1<Boolean, Integer>                   retryPredicate      = (retriesSoFar) -> true;

            public final void setRetryLimit(Integer n) {
                retryPredicate = (retriesSoFar) -> retriesSoFar < n;
            }

            @Override public final void    onIoException     (AutoReconnectingClientIos self, Exception exc) { reconnectExcHandler.accept(self, exc); }
            @Override public final Integer reconnectTimeoutMs() { return reconnectTimeoutMs.apply(); }
            @Override public final Boolean toRetryReconnect  (Integer retriesSoFar) { return retryPredicate.apply(retriesSoFar); }

        }
        static Options defaults() {return new Editable();}
    }
    public static class NotToRetryException extends RuntimeException {
        public final Integer retries;
        public NotToRetryException(Integer retries) {
            super(String.format("retries attempted: %s", retries));
            this.retries = retries;
        }
    }

    private final InetSocketAddress    serverAddr;
    private final Method2<AutoReconnectingClientIos, Exception>
                                       ioExcHandler;
    private final Function0<Integer>   reconnectTimeoutMs;
    private final Function1<Boolean, Integer>
                                       retryPredicate;
    private final Object               supplierSync = new Object();
    private       CloseableIosSupplier iosSupplier;
    private       Boolean              isOk = false;

    private     void reconnect() {
        if (iosSupplier != null) {
            try {
                iosSupplier.close();
            } catch (Exception ex) {/* not closed, not a big deal - discard and keep going */}
        }
        var socket  = Util.getConnectedSocket(serverAddr);
        iosSupplier = CloseableIosSupplier.of(socket);
        isOk = true;
    }
    private <T> T    retriedIo(Function0<T> function) {
        Integer retries = 0;
        while (true) {
            try {
                var x = function.apply();
                isOk = true;
                return x;
            } catch (Exception ex) {
                isOk = false;
                if (!retryPredicate.apply(retries)) {
                    throw new NotToRetryException(retries);
                }
                sleep(reconnectTimeoutMs.apply());
                retries += 1;
                ioExcHandler.accept(this, ex);
                reconnect();
            }
        }
    }

    public AutoReconnectingClientIos(InetSocketAddress serverAddr,
                                     Options           options) {
        this.serverAddr         = serverAddr;
        this.ioExcHandler       = options::onIoException;
        this.reconnectTimeoutMs = options::reconnectTimeoutMs;
        this.retryPredicate     = options::toRetryReconnect;
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }
    public AutoReconnectingClientIos(InetSocketAddress serverAddr) { this(serverAddr, Options.defaults()); }

    public final Boolean isOk() {return isOk;}
    public final void    close() {
        if (iosSupplier != null) {
                iosSupplier.close();
            }
    }

    @Override public InputStream  getInputStream () {
        synchronized (supplierSync) {
            return retriedIo(iosSupplier::getInputStream);
        }
    }
    @Override public OutputStream getOutputStream() {
        synchronized (supplierSync) {
            return retriedIo(iosSupplier::getOutputStream);
        }
    }

}

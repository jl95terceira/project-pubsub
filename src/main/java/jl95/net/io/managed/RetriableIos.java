package jl95.net.io.managed;

import static jl95.lang.SuperPowers.constant;
import static jl95.lang.SuperPowers.ifNull;
import static jl95.lang.SuperPowers.method;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.strict;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import jl95.lang.StrictMap;
import jl95.lang.StrictSet;
import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Function2;
import jl95.lang.variadic.Method0;
import jl95.lang.variadic.Method1;
import jl95.net.io.CloseableIos;
import jl95.net.io.Ios;
import jl95.net.io.managed.util.Defaults;

public abstract class RetriableIos implements ManagedIos {

    private final StrictMap<InetSocketAddress, CloseableIos> iosMapByAddr     = strict(new ConcurrentHashMap<>());
    private final StrictMap<InetSocketAddress, Object> iosReconnectSyncMap = strict(new ConcurrentHashMap<>());
    private final StrictSet<InetSocketAddress> addrsReconnecting = strict(new HashSet<>());
    private       Function0<Integer>           retryTimeoutMs;
    private       Function1<Boolean, Integer>  retryPredicate;
    private       Boolean                      toStopRetries = false;
    private       Integer                      retriesSoFar  = 0;
    private       Function0<Integer>           reconnectTimeoutMs;
    private       Method1<CloseableIos>        onConnection      = ios -> {};

    private <T> T   retried    (Function1<T, Ios> f) {
        while (!toStopRetries) {
            var addr = loadAddress();
            CloseableIos ios = null;
            var gotIosError  = false;
            var deferOnError = method(() -> {});
            try {
                if (iosMapByAddr.containsKey(addr)) {
                    deferOnError = addrRemover(addr);
                    ios = iosMapByAddr.get(addr);
                }
                else {
                    ios = connect(addr);
                    deferOnError = addrRemover(addr);
                }
                return f.apply(ios);
            }
            catch (Exception ex) {
                gotIosError = true;
                deferOnError.accept();
                onIosException(addr, ex);
                if (!ifNull(retryPredicate, n -> true).apply(retriesSoFar)) {
                    throw new NoMoreRetriesException();
                }
                sleep(ifNull(retryTimeoutMs, Defaults.retryTimeoutMs).apply());
                retriesSoFar += 1;
            }
            if (gotIosError) continue;
            retriesSoFar = 0;
        }
        throw new StopRetriesException();
    }
    private Method0 addrRemover(InetSocketAddress addr) {
        return () -> {
            iosMapByAddr.remove(addr);
        };
    }

    protected RetriableIos() {

        Runtime.getRuntime().addShutdownHook(new Thread(this::stopRetries));
    }

    protected abstract InetSocketAddress loadAddress     ();
    protected abstract CloseableIos      connect         (InetSocketAddress addr);
    protected abstract void              onIosException  (InetSocketAddress addr, Exception ex);
    protected          void              retryExecute    (Method0           f) { new Thread(f::accept).start(); }
    protected          void              onToStopRetries () {}

    synchronized
    public final void           put               (InetSocketAddress addr) {
        iosReconnectSyncMap.put(addr, new Object());
        try {
            var ios = connect(addr);
            onConnection.accept(ios);
            iosMapByAddr.put(addr, ios);
        }
        catch (Exception ex) {
            reconnect(addr);
        }
    }
    public final CloseableIos   get               (InetSocketAddress addr) {
        return iosMapByAddr.get(addr);
    }
    public final Iterable<CloseableIos>
                                getAll            () {return iosMapByAddr.values();}
    synchronized
    public final void           forget            (InetSocketAddress addr) {
        iosReconnectSyncMap.remove(addr);
        if (iosMapByAddr.containsKey(addr)) {
            try {
                iosMapByAddr.get(addr).close();
            }
            catch (Exception ex) {/* who cares */}
            iosMapByAddr.remove(addr);
        }
    }
    synchronized
    public final void           reconnect         (InetSocketAddress addr) {
        if (addrsReconnecting.contains(addr)) /* already reconnecting */ {
            return;
        }
        forget(addr);
        iosReconnectSyncMap.put(addr, new Object());
        addrsReconnecting.add(addr);
        retryExecute(() -> {
            synchronized (getReconnectSync(addr)) {
                while (!toStopRetries()) {
                    CloseableIos ios;
                    try {
                        ios = connect(addr);
                        try {
                            onConnection.accept(ios);
                        }
                        catch (Exception ex) {
                            try { ios.close(); }
                            catch (Exception ex_) {/* the show must go on */}
                        }
                        iosMapByAddr.put(addr, ios);
                    } catch (Exception ex) {
                        sleep(ifNull(reconnectTimeoutMs, Defaults.reconnectTimeoutMs).apply());
                        continue;
                    }
                    addrsReconnecting.remove(addr);
                    break;
                }
            }
        });
    }
    synchronized
    public final Object         getReconnectSync  (InetSocketAddress addr) {return iosReconnectSyncMap.get(addr);}
    public final Boolean        isConnected       (InetSocketAddress addr) {
        return iosMapByAddr.containsKey(addr);
    }
    public final void           setOnConnection   (Method1<CloseableIos> m) {
        onConnection = m;
    }
    public final void           setRetryTimeoutMs (Function0<Integer> t) { this.retryTimeoutMs = t; }
    public final void           setRetryTimeoutMs (Integer            t) { setRetryTimeoutMs(constant(t)); }
    public final void           setRetryPredicate (Function1<Boolean, Integer> f) { this.retryPredicate = f; }
    public final void           setRetryLimit     (Integer max) { setRetryPredicate(n -> n <= max); }
    public final Integer        getRetriesSoFar   () {return retriesSoFar;}
    public final void           stopRetries       () {
        toStopRetries = true;
        onToStopRetries();
    }
    public final Boolean        toStopRetries     () { return toStopRetries; }
    public final void           setReconnectTimeoutMs(Function0<Integer> t) { this.reconnectTimeoutMs = t; }
    public final void           setReconnectTimeoutMs(Integer            t) { setReconnectTimeoutMs(constant(t)); }

    @Override public final <T> T withInput (Function1<T, InputStream>  f) {
        return retried((ios) -> f.apply(ios.getInputStream ()));
    }
    @Override public final <T> T withOutput(Function1<T, OutputStream> f) {
        return retried((ios) -> f.apply(ios.getOutputStream()));
    }
    @Override public final <T> T withIo    (Function2<T, InputStream, OutputStream> f) { return retried((ios) -> f.apply(ios.getInputStream(), ios.getOutputStream())); }

    public static class NoMoreRetriesException extends RuntimeException {}
    public static class StopRetriesException   extends RuntimeException {}
}

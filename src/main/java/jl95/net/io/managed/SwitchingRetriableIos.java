package jl95.net.io.managed;

import static jl95.lang.SuperPowers.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import jl95.lang.*;
import jl95.lang.variadic.*;
import jl95.net.io.CloseableIos;
import jl95.net.io.Ios;

public abstract class SwitchingRetriableIos implements ManagedIos {

    public static class NoAddressesException   extends RuntimeException {}
    public static class NoMoreRetriesException extends RuntimeException {}

    private final StrictSet<InetSocketAddress>      peersAddrSet     = strict(Set());
    private final List<InetSocketAddress>           peersAddrList    = new ArrayList<>(1);
    private final StrictMap<InetSocketAddress, CloseableIos> peersIoMapByAddr           = strict(new ConcurrentHashMap<>());
    private final StrictMap<InetSocketAddress, Object>       peersIoReloadSyncMapByAddr = strict(new ConcurrentHashMap<>());
    private final Iterator<InetSocketAddress> peerAddressSwitcher;
    private final ScheduledExecutorService    pool;
    private       InetSocketAddress           peerCurAddress;
    private       Method1<CloseableIos>       onConnection = ios -> {};
    private       Integer                     retryTimeoutMs;
    private       Integer                     retryReconnectTimeoutMs;
    private       Function1<Boolean, Integer> retryPredicate;
    private       Boolean                     toStopRetries = false;

    private <T> T switching(Function1<T, Ios> f) {
        var retriesSoFar = 0;
        while (true) {
            try {
                CloseableIos ios;
                if (peersIoMapByAddr.containsKey(peerCurAddress)) {
                    ios = peersIoMapByAddr.get(peerCurAddress);
                }
                else {
                    throw new Exception();
                }
                try {
                    return f.apply(ios);
                }
                catch (Exception ex) {
                    reconnect(peerCurAddress);
                    throw ex;
                }
            }
            catch (Exception ex) {
                switchIo();
                if (!ifNull(retryPredicate, n -> true).apply(retriesSoFar)) {
                    throw new NoMoreRetriesException();
                }
                sleep(ifNull(retryTimeoutMs, 250));
                retriesSoFar += 1;
            }
        }
    }

    synchronized private void reconnect(InetSocketAddress addr) {
        if (peersIoMapByAddr.containsKey(addr)) {
            try {
                peersIoMapByAddr.get(addr).close();
            }
            catch (Exception ex) {/* who cares */}
            peersIoMapByAddr.remove(addr);
        }
        var sync = peersIoReloadSyncMapByAddr.get(addr);
        pool.execute(() -> {
            synchronized (sync) {
                while (!toStopRetries) {
                    CloseableIos ios;
                    try {
                        ios = getIos(addr);
                        try {
                            onConnection.accept(ios);
                        }
                        catch (Exception ex) {
                            try { ios.close(); }
                            catch (Exception ex_) {/* the show must go on */}
                        }
                        peersIoMapByAddr.put(addr, ios);
                    } catch (Exception ex) {
                        sleep(ifNull(retryReconnectTimeoutMs, 2000));
                        continue;
                    }
                    break;
                }
            }
        });
    }

    protected abstract CloseableIos getIos(InetSocketAddress addr);

    protected SwitchingRetriableIos(Iterable<InetSocketAddress> peerAddresses) {

        I.of(peerAddresses).to(peersAddrSet);
        I.of(peerAddresses).to(peersAddrList);
        for (var addr: peersAddrList) {
            peersIoReloadSyncMapByAddr.put(addr, new Object());
        }
        pool = new ScheduledThreadPoolExecutor(peersAddrList.size());
        if (peersAddrList.isEmpty()) {
            throw new NoAddressesException();
        }
        for (var addr: peersAddrList) {
            try {
                var ios = getIos(addr);
                onConnection.accept(ios);
                peersIoMapByAddr.put(addr, ios);
            }
            catch (Exception ex) {
                reconnect(addr);
            }
        }
        peerAddressSwitcher = I.of(peersAddrList).cycle().iterator();
        peerCurAddress = peerAddressSwitcher.next();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            toStopRetries = true;
        }));
    }

    public final void switchIo         () {
        peerCurAddress = peerAddressSwitcher.next();
    }
    public final void setOnConnection  (Method1<CloseableIos> m) {
        onConnection = m;
    }
    public final void setRetryTimeoutMs(Integer t) { this.retryTimeoutMs = t; }
    public final void setRetryPredicate(Function1<Boolean, Integer> f) { this.retryPredicate = f; }
    public final void setRetryLimit    (Integer max) { setRetryPredicate(n -> n <= max); }
    public final void closeAll         () {
        toStopRetries = true;
        for (var addr: peersAddrList) {
            var sync = peersIoReloadSyncMapByAddr.get(addr);
            synchronized (sync) {/* wait stop */}
        }
        for (var ios: peersIoMapByAddr.values()) {
            ios.close();
        }
    }

    @Override public final <T> T withInput (Function1<T, InputStream>  f) {
        return switching((ios) -> f.apply(ios.getInputStream ()));
    }
    @Override public final <T> T withOutput(Function1<T, OutputStream> f) {
        return switching((ios) -> f.apply(ios.getOutputStream()));
    }
    @Override public final <T> T withIo    (Function2<T, InputStream, OutputStream> f) { return switching((ios) -> f.apply(ios.getInputStream(), ios.getOutputStream())); }
}

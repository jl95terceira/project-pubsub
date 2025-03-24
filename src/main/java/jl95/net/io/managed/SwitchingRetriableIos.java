package jl95.net.io.managed;

import static jl95.lang.SuperPowers.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import jl95.lang.I;
import jl95.lang.StrictSet;
import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method0;
import jl95.lang.variadic.Method1;
import jl95.net.io.CloseableIos;
import jl95.net.io.managed.util.Defaults;

public abstract class SwitchingRetriableIos extends RetriableIos {

    public static class NoAddressesException      extends RuntimeException {}
    public static class NoMoreReswitchesException extends RuntimeException {}

    private final StrictSet<InetSocketAddress> addrsSet = strict(Set());
    private final List<InetSocketAddress>      addrsList = new ArrayList<>(1);
    private final Iterator<InetSocketAddress>  peerAddressSwitcher;
    private final ScheduledExecutorService     pool;
    private       InetSocketAddress            peerCurAddress;
    private       Function1<Boolean, Integer>  reswitchPredicate = null;
    private       Function0<Integer>           reswitchTimeoutMs = null;

    private Integer reswitchIo(Integer reswitchesSoFar) {
        switchIo();
        if (!ifNull(reswitchPredicate, i -> true).apply(reswitchesSoFar)) {
            throw new NoMoreRetriesException();
        }
        sleep(ifNull(reswitchTimeoutMs, Defaults.reswitchTimeoutMs).apply());
        return reswitchesSoFar + 1;
    }

    protected SwitchingRetriableIos(Iterable<InetSocketAddress> peerAddresses) {

        I.of(peerAddresses).to(addrsSet);
        I.of(peerAddresses).to(addrsList);
        pool = new ScheduledThreadPoolExecutor(addrsList.size());
        if (addrsList.isEmpty()) {
            throw new NoAddressesException();
        }
        for (var addr: addrsList) {
            put(addr);
        }
        peerAddressSwitcher = I.of(addrsList).cycle().iterator();
        peerCurAddress = peerAddressSwitcher.next();
    }

    @Override protected final InetSocketAddress loadAddress    () {

        var reswitchesSoFar = 0;
        while (ifNull(reswitchPredicate, i -> true).apply(reswitchesSoFar)) {
            if (isConnected(peerCurAddress)) {
                return peerCurAddress;
            } else {
                reswitchesSoFar = reswitchIo(reswitchesSoFar);
                continue;
            }
        }
        throw new NoMoreReswitchesException();
    }
    @Override protected final void              onIosException (InetSocketAddress addr, Exception ex) {
        reconnect(addr);
        reswitchIo(0);
    }
    @Override protected final void              retryExecute   (Method0 f) { pool.execute(f::accept); }
    @Override protected final void              onToStopRetries() {
        for (var addr: addrsList) {
            var sync = getReconnectSync(addr);
            if (sync != null) {
                synchronized (getReconnectSync(addr)) {/* wait stop */}
            }
        }
        for (var ios: getAll()) {
            ios.close();
        }
    }

    public final void switchIo             () {
        peerCurAddress = peerAddressSwitcher.next();
    }
    public final void setReswitchPredicate (Function1<Boolean, Integer> f) {
        reswitchPredicate = f;}
    public final void setReswitchLimit     (Integer max) {setReswitchPredicate(i -> i <= max);}
    public final void setReswitchTimeoutMs (Function0<Integer>          f) {reswitchTimeoutMs = f;}
    public final void closeAll             () {
        stopRetries();
    }
}

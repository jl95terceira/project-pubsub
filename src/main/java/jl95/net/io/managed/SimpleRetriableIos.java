package jl95.net.io.managed;

import jl95.lang.I;
import jl95.lang.StrictSet;
import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method0;
import jl95.net.io.managed.util.Defaults;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static jl95.lang.SuperPowers.*;

public abstract class SimpleRetriableIos extends RetriableIos {

    private final InetSocketAddress peerAddress;
    private       Method0           reconnectHandler = null;

    protected SimpleRetriableIos(InetSocketAddress peerAddress) {

        this.peerAddress = peerAddress;
        put(peerAddress);
    }

    public final void reconnect() { reconnect(peerAddress); }

    @Override protected final InetSocketAddress loadAddress    () {
        return peerAddress;
    }
    @Override protected final void              onIosException (InetSocketAddress addr, Exception ex) {

        reconnect(addr);
        ifNull(reconnectHandler, () -> {}).accept();
    }
    @Override protected final void              retryExecute   (Method0 f) {
        f.accept();
    }

    public final void setReconnectHandler(Method0 f) {reconnectHandler = f;}
}

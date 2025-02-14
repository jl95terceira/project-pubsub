package jl95.net;

import static jl95.lang.SuperPowers.ifNull;
import static jl95.lang.SuperPowers.uncheck;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import jl95.lang.Awaitable;
import jl95.lang.variadic.*;

public class Server {

    private final ServerSocket               serverSocket;
    private final ThreadPoolExecutor         pool = new ScheduledThreadPoolExecutor(4);
    private       Method2<Server, Socket>    acceptCb;
    private       Method2<Server, Exception> acceptErrorCb;
    private       Method1<Server>            acceptTimeoutCb;
    private       Boolean                    isRunning = false;
    private       Boolean                    toStop    = false;
    private       CompletableFuture<Void>    startFuture;
    private       CompletableFuture<Void>    stopFuture;


    public Server(ServerSocket socket) {
        this.serverSocket    = socket;
    }

    public final void setAcceptCb       (Method2<Server, Socket>    cb) {
        acceptCb        = cb;
    }
    public final void setAcceptErrorCb  (Method2<Server, Exception> cb) {
        acceptErrorCb   = cb;
    }
    public final void setAcceptTimeoutCb(Method1<Server>            cb) {
        acceptTimeoutCb = cb;
    }

    synchronized public final Awaitable<Void> start    () {

        if (isRunning()) throw new IllegalStateException();
        toStop      = false;
        startFuture = new CompletableFuture<>();
        stopFuture  = new CompletableFuture<>();
        new Thread(() -> {
            startFuture.complete(null);
            while (!toStop) {
                try {
                    java.net.Socket socket;
                    try {
                        socket = serverSocket.accept();
                    }
                    catch (java.net.SocketTimeoutException ex) /* not really an error - just to give control back to the thread every so often */ {
                        ifNull(acceptTimeoutCb, (self) -> {}).accept(this);
                        continue;
                    }
                    catch (Exception ex) {
                        ifNull(acceptErrorCb, (self, ex_) -> { System.out.printf("Error on accept: %s\n", ex_); }).accept(this, ex);
                        continue;
                    }
                    pool.execute(() -> ifNull(acceptCb, (self, socket_) -> {}).accept(this, socket));
                }
                catch (Exception ex) /* happened in non-final (overridable) methods */ {
                    ex.printStackTrace();
                }
            }
            stopFuture.complete(null);
            isRunning = false;
        }).start();
        isRunning = true;
        return Awaitable.of(startFuture);
    }
    synchronized public final Awaitable<Void> stop     () {

        if (!isRunning()) throw new IllegalStateException();
        if (stopFuture == null) throw new AssertionError();
        toStop = true;
        return Awaitable.of(stopFuture);
    }
    synchronized public final Boolean         isRunning() { return isRunning; }
    synchronized public final ServerSocket    getSocket() { return serverSocket; }

    public void close() {
        uncheck(getSocket()::close);
    }
}

package jl95.rpc;

import static jl95.lang.SuperPowers.uncheck;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Function2;
import jl95.net.Io;
import jl95.net.Receiver;
import jl95.net.Sender;
import jl95.rpc.util.Request;
import jl95.rpc.util.Response;
import jl95.rpc.util.SerdesDefaults;

@FunctionalInterface
public interface RequesterFunction<A, R>
    extends Function2<R, A, Requester.SendOptions<A, R>>,
            Function1<R, A> {

    default R apply(A requestObject) { return apply(requestObject, Requester.SendOptions.defaults()); }
}

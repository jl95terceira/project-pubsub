package jl95.net.rpc;

import static jl95.lang.SuperPowers.self;
import static jl95.lang.SuperPowers.tuple;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Tuple2;

public interface ResponderIf<A, R> {

    Awaitable<Void> respondWhile(Function1<Tuple2<R, Boolean>, A> responseFunction);
    Awaitable<Void> stop        ();
    Boolean         isRunning   ();

    default Awaitable<Void> respond    (Function1<R, A> responseFunction) {
        return respondWhile(request -> tuple(responseFunction.apply(request), true));
    }
    default Awaitable<Void> respondOnce(Function1<R, A> responseFunction) {
        return respondWhile(request -> tuple(responseFunction.apply(request), false));
    }
    default <A2, R2> ResponderIf<A2, R2> adapted        (Function1<A2, A> requestAdapter,
                                                         Function1<R, R2> responseAdapter) {
        return new ResponderIf<>() {

            @Override public Awaitable<Void> respondWhile(Function1<Tuple2<R2, Boolean>, A2> responseFunction) {
                return ResponderIf.this.respondWhile(request -> {
                    var adaptedRequest = requestAdapter.apply(request);
                    var response = responseFunction.apply(adaptedRequest);
                    return tuple(responseAdapter.apply(response.a1), response.a2);
                });
            }
            @Override public Awaitable<Void> stop        () {
                return ResponderIf.this.stop();
            }
            @Override public Boolean         isRunning   () {
                return ResponderIf.this.isRunning();
            }
        };
    }
    default <A2>     ResponderIf<A2, R>  adaptedRequest (Function1<A2, A> requestAdapter) {

        return adapted(requestAdapter, self::apply);
    }
    default <R2>     ResponderIf<A, R2>  adaptedResponse(Function1<R, R2> responseAdapter) {

        return adapted(self::apply, responseAdapter);
    }
}

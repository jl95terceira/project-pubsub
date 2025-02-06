package jl95.rpc;

import static jl95.lang.SuperPowers.uncheck;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;

public interface ResponderIf<A, R> {

    Awaitable<Void> start    (Function1<R, A> responseFunction);
    Awaitable<Void> stop     ();
    Boolean         isRunning();

    default <A2, R2> ResponderIf<A2, R2> adapted(Function1<A2, A> argAdapter,
                                               Function1<R, R2> reAdapter) {
        return new ResponderIf<A2, R2>() {

            @Override public Awaitable<Void> start    (Function1<R2, A2> responseFunction) {
                return ResponderIf.this.start(a -> reAdapter.apply(responseFunction.apply(argAdapter.apply(a))));
            }
            @Override public Awaitable<Void> stop     () { return ResponderIf.this.stop     (); }
            @Override public Boolean         isRunning() { return ResponderIf.this.isRunning(); }
        };
    }
}

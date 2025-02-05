package jl95.rpc;

import static jl95.lang.SuperPowers.uncheck;

import java.util.concurrent.Future;

import jl95.lang.variadic.Function1;

public interface Responder<A, R> {

    void         start    (Function1<R, A> responseFunction);
    Future<Void> stop     ();
    void         stopAwait();
    Boolean      isRunning();

    default <A2, R2> Responder<A2, R2> adapted(Function1<A2, A> argAdapter,
                                               Function1<R, R2> reAdapter) {
        return new Responder<A2, R2>() {

            @Override public void         start    (Function1<R2, A2> responseFunction) {
                Responder.this.start(a -> reAdapter.apply(responseFunction.apply(argAdapter.apply(a))));
            }
            @Override public Future<Void> stop     () { return Responder.this.stop     (); }
            @Override public void         stopAwait() {        Responder.this.stopAwait(); }
            @Override public Boolean      isRunning() { return Responder.this.isRunning(); }
        };
    }
}

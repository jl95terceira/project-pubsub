package jl95.rpc;

import static jl95.lang.SuperPowers.uncheck;

import jl95.lang.variadic.Function1;

public interface Requester<A, R> {

    R apply(A requestObject);

    default <A2, R2> Requester<A2, R2> adapted(Function1<A, A2> argAdapter,
                                                 Function1<R2, R> reAdapter) {
        return requestObject -> reAdapter.apply(Requester.this.apply(argAdapter.apply(requestObject)));
    }
}

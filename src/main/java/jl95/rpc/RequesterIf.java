package jl95.rpc;

import jl95.lang.variadic.Function1;

public interface RequesterIf<A, R> {

    R apply(A requestObject, Requester.SendOptions options);

    default R apply(A requestObject) { return apply(requestObject, Requester.SendOptions.defaults()); }
    default <A2, R2> RequesterIf<A2, R2> adapted(Function1<A, A2> argAdapter,
                                                 Function1<R2, R> reAdapter) {
        return (requestObject, options) -> reAdapter.apply(RequesterIf.this.apply(argAdapter.apply(requestObject), options));
    }
}

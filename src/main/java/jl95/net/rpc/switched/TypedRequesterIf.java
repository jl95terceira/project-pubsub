package jl95.net.rpc.switched;

import static jl95.lang.SuperPowers.self;

import jl95.lang.variadic.Function1;
import jl95.net.rpc.RequesterIf;

public interface TypedRequesterIf<A, R> {

    RequesterIf<A, R> getFunction(String typeAlias);

    default <A2, R2> TypedRequesterIf<A2, R2> adapted(Function1<A, A2> reqAdapter,
                                                      Function1<R2, R> resAdapter) {
        return typeAlias -> getFunction(typeAlias).adapted(reqAdapter, resAdapter);
    }
}

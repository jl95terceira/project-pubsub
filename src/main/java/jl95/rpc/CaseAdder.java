package jl95.rpc;

import jl95.lang.variadic.Function1;

@FunctionalInterface
public interface CaseAdder {

    <A, R> void addCase(String          typeAlias,
                        Function1<R, A> responseFunction);
}

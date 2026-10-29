package jl95.net.rpc.switched;

import static jl95.lang.SuperPowers.self;
import static jl95.lang.SuperPowers.tuple;

import jl95.lang.variadic.*;
import jl95.net.rpc.Responder;
import jl95.net.rpc.util.TypedPayload;
import jl95.util.UVoidFuture;

public interface TypeSwitchedResponder<ABase, RBase> {

    void        addCaseWhile  (String typeAlias,
                               Function1<Tuple2<RBase, Boolean>, ABase> responseFunction);
    void        removeCase    (String typeAlias);
    void        setDefaultCase(Function1<Tuple2<RBase, Boolean>, ABase> responseFunction);
    UVoidFuture start         ();
    UVoidFuture stop          ();
    Boolean     isRunning     ();
    Responder<TypedPayload, TypedPayload> getBaseResponder();

    default void
    addCase        (String                   typeAlias,
                    Function1<RBase, ABase>  responseFunction) {
        addCaseWhile(typeAlias, (ABase a) -> tuple(responseFunction.apply(a), true));
    }
    default void
    addCaseBreak   (String                   typeAlias,
                    Function1<RBase, ABase>  responseFunction) {
        addCaseWhile(typeAlias, (ABase a) -> tuple(responseFunction.apply(a), false));
    }
    default <ABase2, RBase2> TypeSwitchedResponder<ABase2, RBase2>
    adapted        (Function1<ABase2, ABase> requestAdapter,
                    Function1<RBase, RBase2> responseAdapter) {
        return new TypeSwitchedResponder<>() {

            @Override
            public void addCaseWhile(String typeAlias, Function1<Tuple2<RBase2, Boolean>, ABase2> responseFunction) {
                TypeSwitchedResponder.this.addCaseWhile(typeAlias, a -> {
                    var r = responseFunction.apply(requestAdapter.apply(a));
                    return tuple(responseAdapter.apply(r.a1), r.a2);
                });
            }

            @Override
            public void removeCase(String typeAlias) {
                TypeSwitchedResponder.this.removeCase(typeAlias);
            }

            @Override
            public void setDefaultCase(Function1<Tuple2<RBase2, Boolean>, ABase2> responseFunction) { TypeSwitchedResponder.this.setDefaultCase(a -> {
                    var r = responseFunction.apply(requestAdapter.apply(a));
                    return tuple(responseAdapter.apply(r.a1), r.a2);
                }); }

            @Override
            public UVoidFuture start() {
                return TypeSwitchedResponder.this.start();
            }

            @Override
            public UVoidFuture stop() {
                return TypeSwitchedResponder.this.stop();
            }

            @Override
            public Boolean isRunning() {
                return TypeSwitchedResponder.this.isRunning();
            }

            @Override
            public Responder<TypedPayload, TypedPayload> getBaseResponder() {
                return TypeSwitchedResponder.this.getBaseResponder();
            }
        };
    }
    default <ABase2> TypeSwitchedResponder<ABase2, RBase>
    adaptedRequest (Function1<ABase2, ABase> requestAdapter) {

        return adapted(requestAdapter, self::apply);
    }
    default <RBase2> TypeSwitchedResponder<ABase, RBase2>
    adaptedResponse(Function1<RBase, RBase2> responseAdapter) {

        return adapted(self::apply, responseAdapter);
    }
}


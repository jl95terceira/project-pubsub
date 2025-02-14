package jl95.rpc;

import static jl95.lang.SuperPowers.self;

import java.util.HashMap;
import java.util.Map;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
import jl95.net.Ios;
import jl95.rpc.util.TypedPayload;
import jl95.rpc.util.serdes.TypedPayloadJsonSerdes;

public interface TypeSwitchedResponderIf<ABase, RBase> {

    void            addCase   (String                  typeAlias,
                               Function1<RBase, ABase> responseFunction);
    void            removeCase(String                  typeAlias);
    Awaitable<Void> start     ();
    Awaitable<Void> stop      ();
    Boolean         isRunning ();
    ResponderIf<TypedPayload, TypedPayload> getBaseResponder();

    default  <As2, Rs2> TypeSwitchedResponderIf<As2, Rs2> adapted(Function1<As2, ABase> requestAdapter,
                                                                  Function1<RBase, Rs2> responseAdapter) {
        return new TypeSwitchedResponderIf<>() {

            @Override
            public void addCase(String typeAlias, Function1<Rs2, As2> responseFunction) {
                TypeSwitchedResponderIf.this.addCase(typeAlias, r -> responseAdapter.apply(responseFunction.apply(requestAdapter.apply(r))));
            }

            @Override
            public void removeCase(String typeAlias) {
                TypeSwitchedResponderIf.this.removeCase(typeAlias);
            }

            @Override
            public Awaitable<Void> start() {
                return TypeSwitchedResponderIf.this.start();
            }

            @Override
            public Awaitable<Void> stop() {
                return TypeSwitchedResponderIf.this.stop();
            }

            @Override
            public Boolean isRunning() {
                return TypeSwitchedResponderIf.this.isRunning();
            }

            @Override
            public ResponderIf<TypedPayload, TypedPayload> getBaseResponder() {
                return TypeSwitchedResponderIf.this.getBaseResponder();
            }
        };
    }
}


package jl95.rpc;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;

public interface TsrGenericIf<As, Rs> {
    <A, R> void addCase(String           typeAlias,
                        Function1<R, A>  responseFunction,
                        Function1<A, As> requestReader,
                        Function1<Rs, R> responseWriter);
    void            removeCase(String               typeAlias);
    Awaitable<Void> start     ();
    Awaitable<Void> stop      ();
    Boolean         isRunning ();

    default <As2, Rs2> TsrGenericIf<As2, Rs2> adapted(Function1<As2, As> requestAdapter, Function1<Rs, Rs2> responseAdapter) {
        return new TsrGenericIf<>() {

            @Override public <A, R> void     addCase   (String typeAlias, Function1<R, A> responseFunction, Function1<A, As2> requestReader,
                    Function1<Rs2, R> responseWriter) {
                TsrGenericIf.this.addCase(typeAlias, responseFunction, reqSerial -> requestReader.apply(requestAdapter.apply(reqSerial)), res -> responseAdapter.apply(responseWriter.apply(res)));
            }
            @Override public void            removeCase(String typeAlias) { TsrGenericIf.this.removeCase(typeAlias); }
            @Override public Awaitable<Void> start     () {
                return TsrGenericIf.this.start    ();
            }
            @Override public Awaitable<Void> stop      () {
                return TsrGenericIf.this.stop     ();
            }
            @Override public Boolean         isRunning () {
                return TsrGenericIf.this.isRunning();
            }
        };
    }
}

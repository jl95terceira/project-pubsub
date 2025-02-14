package jl95.net.rpc;

import static jl95.lang.SuperPowers.uncheck;

import java.io.InputStream;
import java.io.OutputStream;

import jl95.lang.variadic.*;
import jl95.net.rpc.util.Defaults;

public interface RequesterIf<A, R> {

    public interface SendOptions {

        Integer getResponseTimeoutMs();
        void    onOutOfSync         ();

        class Editable implements SendOptions {

            public Function0<Integer> responseTimeoutMs = () -> Defaults.responseTimeoutMs;
            public Method0 outOfSyncHandler  = () -> {};

            @Override public Integer getResponseTimeoutMs() { return responseTimeoutMs.apply (); }
            @Override public void    onOutOfSync         () { outOfSyncHandler        .accept(); }
        }
        static SendOptions defaults() {
            return new Editable();
        }
    }

    R            apply(A requestObject, SendOptions options);
    InputStream  getInputStream ();
    OutputStream getOutputStream();

    default R apply(A requestObject) { return apply(requestObject, SendOptions.defaults()); }
    default <A2, R2> RequesterIf<A2, R2> adapted(Function1<A, A2> reqAdapter,
                                                 Function1<R2, R> resAdapter) {
        return new RequesterIf<>() {

            @Override public R2           apply          (A2 requestObject, SendOptions options) {
                var adaptedRequestObject = reqAdapter.apply(requestObject);
                var reponseObject = RequesterIf.this.apply(adaptedRequestObject, options);
                return resAdapter.apply(reponseObject);
            }
            @Override public InputStream  getInputStream () {
                return RequesterIf.this.getInputStream();
            }
            @Override public OutputStream getOutputStream() {
                return RequesterIf.this.getOutputStream();
            }
        };
    }
}

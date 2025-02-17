package jl95.net.io;

import java.io.OutputStream;

import jl95.lang.variadic.Function1;

public interface SenderIf<T> {

    void         send           (T outgoing);
    OutputStream getOutputStream();

    default <T2> SenderIf<T2> adaptedSender(Function1<T, T2> adapterFunction) {

        return new SenderIf<>() {

            @Override public void send(T2 outgoing) {
                var adaptedOutgoing = adapterFunction.apply(outgoing);
                SenderIf.this.send(adaptedOutgoing);
            }
            @Override public OutputStream getOutputStream() {
                return SenderIf.this.getOutputStream();
            }
        };
    }
}
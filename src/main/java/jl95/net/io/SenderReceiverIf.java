package jl95.net.io;

import static jl95.lang.SuperPowers.constant;

import java.net.Socket;

import jl95.lang.variadic.Function0;
import jl95.net.io.managed.ManagedIos;

public interface SenderReceiverIf<S, R> {

    SenderIf<S>   getSender  ();
    ReceiverIf<R> getReceiver();

    static <S, R> SenderReceiverIf<S, R> of        (Function0<SenderIf<S>>   s,
                                                    Function0<ReceiverIf<R>> r) {
        return new SenderReceiverIf<>() {
            @Override
            public SenderIf<S> getSender() {
                return s.apply();
            }

            @Override
            public ReceiverIf<R> getReceiver() {
                return r.apply();
            }
        };
    }
    static <S, R> SenderReceiverIf<S, R> ofConstant(SenderIf<S>   s,
                                                    ReceiverIf<R> r) {
        return of(constant(s), constant(r));
    }
    static SenderReceiverIf<byte[], byte[]> fromIo        (Ios    ios) {
        return ofConstant(Sender.of(ios.getOutputStream()), Receiver.of(ios.getInputStream()));
    }
    static SenderReceiverIf<byte[], byte[]> fromSocket    (Socket socket) {
        return fromIo(Ios.fromSocket(socket));
    }
    static SenderReceiverIf<byte[], byte[]> fromSocketLazy(Socket socket) {
        return fromIo(Ios.fromSocketLazy(socket));
    }
    static SenderReceiverIf<byte[], byte[]> fromManagedIo (ManagedIos ios) {
        return ofConstant(Sender.of(ios), Receiver.of(ios));
    }
}

package jl95.net.sr;

public interface SrIf<S, R> {

    SenderIf<S> getSender();
    ReceiverIf<R> getReceiver();

    static <S, R> SrIf<S, R> of(SenderIf<S> s, ReceiverIf<R> r) {
        return new SrIf<S, R>() {
            @Override
            public SenderIf<S> getSender() {
                return s;
            }

            @Override
            public ReceiverIf<R> getReceiver() {
                return r;
            }
        };
    }
    static SrIf<byte[], byte[]> of(Ios ios) {
        return of(Sender.of(ios.getOutputStream()), Receiver.of(ios.getInputStream()));
    }
}

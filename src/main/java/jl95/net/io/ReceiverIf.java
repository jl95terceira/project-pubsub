package jl95.net.io;

import static jl95.lang.SuperPowers.constant;

import java.io.InputStream;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function0;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method0;
import jl95.lang.variadic.Method1;

public interface ReceiverIf<T> {

    interface RecvOptions {

        void    afterStop          ();
        void    onInputException   (Exception ex);
        void    onHandlingException(Exception ex);
        void    onInputTimeout     ();
        Integer inputRetryTimeoutMs();
        
        class Editable implements RecvOptions {

            public Method0              afterStop           = ()   -> {};
            public Method1<Exception>   inputExcHandler     = (ex) -> System.out.printf("Exception on reading input: %s%n", ex);
            public Method1<Exception>   handlingExcHandler  = (ex) -> System.out.printf("Exception on handling input: %s%n", ex);
            public Method0              inputTimeoutHandler = ()   ->  {};
            public Function0<Integer>   inputRetryTimeoutMs = constant(50);

            @Override public void afterStop          ()             { afterStop          .accept(); }
            @Override public void onHandlingException(Exception ex) { handlingExcHandler .accept(ex); }
            @Override public void onInputException   (Exception ex) { inputExcHandler    .accept(ex); }
            @Override public void onInputTimeout     ()             { inputTimeoutHandler.accept(); }
            @Override public Integer inputRetryTimeoutMs()          { return inputRetryTimeoutMs.apply(); }
        }
        static RecvOptions defaults() {
            return new RecvOptions.Editable();
        }
    }

    Awaitable<Void> recvWhile     (Function1<Boolean, T> incomingCbToContinue,
                                   RecvOptions           options);
    Awaitable<Void> recvStop      ();
    Boolean         isReceiving   ();
    InputStream     getInputStream();

    default Awaitable<Void> recvWhile    (Function1<Boolean, T> incomingCbToContinue) {

        return recvWhile(incomingCbToContinue, RecvOptions.defaults());
    }
    default Awaitable<Void> recv         (Method1<T>            incomingCb,
                                          RecvOptions           options) {
        return recvWhile(incoming -> {
            incomingCb.accept(incoming);
            return true;
        }, options);
    }
    default Awaitable<Void> recv         (Method1<T>            incomingCb) {

        return recv(incomingCb, RecvOptions.defaults());
    }
    default Awaitable<Void> recvOnce     (Method1<T>            incomingCb,
                                          RecvOptions           options) {
        return recvWhile(incoming -> {
            incomingCb.accept(incoming);
            return false;
        }, options);
    }
    default Awaitable<Void> recvOnce     (Method1<T>            incomingCb) {
        return recvOnce(incomingCb, RecvOptions.defaults());
    }
    default void            ensureStopped() {
        try {
            recvStop().await();
        }
        catch (Receiver.NotReceivingException ex) {
            return;
        }
    }
    default <T2> ReceiverIf<T2> adaptedReceiver(Function1<T2, T> adapterFunction) {
        return new ReceiverIf<>() {

            @Override public Awaitable<Void>  recvWhile     (Function1<Boolean, T2> incomingCbToContinue, RecvOptions options) {
                return ReceiverIf.this.recvWhile(incoming -> {
                    var adaptedIncoming = adapterFunction.apply(incoming);
                    return incomingCbToContinue.apply(adaptedIncoming);
                }, options);
            }
            @Override public Awaitable<Void>  recvStop      () {
                return ReceiverIf.this.recvStop();
            }
            @Override public Boolean          isReceiving   () {
                return ReceiverIf.this.isReceiving();
            }
            @Override public InputStream      getInputStream() {
                return ReceiverIf.this.getInputStream();
            }
        };
    }
}

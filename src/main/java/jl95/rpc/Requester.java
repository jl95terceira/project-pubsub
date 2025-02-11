package jl95.rpc;

import static jl95.lang.SuperPowers.constant;
import static jl95.lang.SuperPowers.uncheck;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jl95.lang.variadic.*;
import jl95.net.Io;
import jl95.net.Receiver;
import jl95.net.Sender;
import jl95.rpc.util.Defaults;
import jl95.rpc.util.Request;
import jl95.rpc.util.Response;
import jl95.rpc.util.SerdesDefaults;

public abstract class Requester<A, R> implements RequesterFunction<A, R> {

    private enum         ResponseExceptionalStatus {
        FAIL_TIMEOUT;
    }
    private static class ResponseStatusAndData {
        public ResponseExceptionalStatus status   = null;
        public Response response;
    }

    public interface SendOptions<A, R> {

        Integer getResponseTimeoutMs(Requester<A, R> self);
        void    onOutOfSync         (Requester<A, R> self);

        class Editable<A, R> implements SendOptions<A, R> {

            public Function1<Integer, Requester<A, R>> responseTimeoutMs = self -> Defaults.responseTimeoutMs;
            public Method1           <Requester<A, R>> outOfSyncHandler  = self -> {};

            @Override public Integer getResponseTimeoutMs(Requester<A, R> self) { return responseTimeoutMs.apply (self); }
            @Override public void    onOutOfSync         (Requester<A, R> self) { outOfSyncHandler        .accept(self); }
        }
        static <A, R> SendOptions<A, R> defaults() {
            return new Editable<>();
        }
    }
    public static class ResponseTimeoutException extends RuntimeException {}

    private final Sender  <Request>        sender;
    private final Receiver<Response>       receiver;
    private final ScheduledExecutorService scheduler         = Executors.newScheduledThreadPool(1);

    protected abstract byte[] writeRequest(A      object);
    protected abstract R      readResponse(byte[] serial);

    private Requester(Sender  <Request>  sender,
                      Receiver<Response> receiver) {this.sender = sender; this.receiver = receiver;}
    public  Requester(Io      io) {

        this.sender   = new Sender  <>(io.getOutputStream()) {
            @Override protected byte[] toBytes(Request outgoing) {
                return SerdesDefaults.requestToBytes.apply(outgoing);
            }
        };
        this.receiver = new Receiver<>(io.getInputStream()) {
            @Override protected Response fromBytes(byte[] incoming) {
                return SerdesDefaults.responseFromBytes.apply(incoming);
            }
        };
    }

    @Override synchronized public final R apply(A requestObject, SendOptions<A, R> options) {

        var request     = new Request();
        request.id      = UUID.randomUUID();
        request.payload = writeRequest(requestObject);
        sender.send(request);
        var responseSync   = new Object();
        var responseFuture = new CompletableFuture<ResponseStatusAndData>();
        receiver.recvWhile(response -> {
            synchronized (responseSync) {
                try {
                    if (responseFuture.isDone()) /* oof, just timed out */ {
                        return false;
                    }
                    var rsd = new ResponseStatusAndData();
                    if (!response.requestId.equals(request.id)) {
                        options.onOutOfSync(this);
                        return true; // discard response (old, out of sync), wait for next
                    }
                    else {
                        rsd.response = response;
                    }
                    responseFuture.complete(rsd);
                    return false;
                }
                catch (Exception ex) {
                    return false;
                }
            }
        });
        scheduler.schedule(() -> {
            synchronized (responseSync) {
                if (responseFuture.isDone()) return;
                // not completed - set failed (by time-out)
                receiver.recvStop().await();
                var rsd = new ResponseStatusAndData();
                rsd.status = ResponseExceptionalStatus.FAIL_TIMEOUT;
                responseFuture.complete(rsd);
            }
        }, options.getResponseTimeoutMs(this), TimeUnit.MILLISECONDS);
        var rsd = uncheck(() -> responseFuture.get());
        if (rsd.status == ResponseExceptionalStatus.FAIL_TIMEOUT) {
            throw new ResponseTimeoutException();
        }
        return readResponse(rsd.response.payload);
    }
    @Override synchronized public final R apply(A requestObject) { return apply(requestObject, SendOptions.defaults()); }

    public final <A2, R2> Requester<A2, R2> adapted(Function1<A, A2> argAdapter,
                                                    Function1<R2, R> reAdapter) {
        return new Requester<A2, R2>(sender, receiver) {

            @Override protected byte[] writeRequest(A2 object) {
                return Requester.this.writeRequest(argAdapter.apply(object));
            }
            @Override protected R2 readResponse(byte[] serial) {
                return reAdapter.apply(Requester.this.readResponse(serial));
            }
        };
    }
    public final          InputStream       getInputStream () { return receiver.getInputStream (); }
    public final          OutputStream      getOutputStream() { return sender  .getOutputStream(); }
}

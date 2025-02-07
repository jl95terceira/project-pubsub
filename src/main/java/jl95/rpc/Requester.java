package jl95.rpc;

import static jl95.lang.SuperPowers.uncheck;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jl95.net.Io;
import jl95.net.Receiver;
import jl95.net.Sender;
import jl95.rpc.util.Request;
import jl95.rpc.util.Response;
import jl95.rpc.util.SerdesDefaults;

public abstract class Requester<A, R> implements RequesterIf<A, R> {

    private enum         ResponseExceptionalStatus {
        FAIL_UNSYNCHRONIZED,
        FAIL_TIMEOUT;
    }
    private static class ResponseStatusAndData {
        public ResponseExceptionalStatus status   = null;
        public Response response;
    }

    public interface SendOptions {

        Integer getResponseTimeoutMs();

        class Editable implements SendOptions {

            public Integer responseTimeoutMs = jl95.rpc.util.Defaults.responseTimeoutMs;

            @Override public Integer getResponseTimeoutMs() { return responseTimeoutMs; }
        }
        static SendOptions defaults() {
            return new Editable();
        }
    }
    public static class ResponseTimeoutException extends RuntimeException {}
    public static class ResponseDesynchException extends RuntimeException {}

    private final Sender  <Request>        sender;
    private final Receiver<Response>       receiver;
    private final ScheduledExecutorService scheduler         = Executors.newScheduledThreadPool(1);

    protected abstract byte[] writeRequest(A      object);
    protected abstract R      readResponse(byte[] serial);

    public Requester(Io      io) {

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

    @Override synchronized public final R apply(A requestObject, SendOptions options) {

        var request     = new Request();
        request.id      = UUID.randomUUID();
        request.payload = writeRequest(requestObject);
        sender.send(request);
        var responseSync   = new Object();
        var responseFuture = new CompletableFuture<ResponseStatusAndData>();
        receiver.recvWhile(response -> {
            synchronized (responseSync) {
                if (responseFuture.isDone()) /* oof, just timed out */ return false;
                var rsd = new ResponseStatusAndData();
                if (!response.requestId.equals(request.id)) {
                    rsd.status = ResponseExceptionalStatus.FAIL_UNSYNCHRONIZED;
                }
                else {
                    rsd.response = response;
                }
                responseFuture.complete(rsd);
                return false;
            }
        });
        scheduler.schedule(() -> {
            synchronized (responseSync) {
                if (responseFuture.isDone()) return;
                // not completed - set failed (by time-out)
                var rsd = new ResponseStatusAndData();
                rsd.status = ResponseExceptionalStatus.FAIL_TIMEOUT;
                responseFuture.complete(rsd);
            }
        }, options.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
        var rsd = uncheck(() -> responseFuture.get());
        if (rsd.status == ResponseExceptionalStatus.FAIL_TIMEOUT) {
            throw new ResponseTimeoutException();
        }
        if (rsd.status == ResponseExceptionalStatus.FAIL_UNSYNCHRONIZED) {
            throw new ResponseDesynchException();
        }
        var responseObject = readResponse(rsd.response.payload);
        return responseObject;
    }
}

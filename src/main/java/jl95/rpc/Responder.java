package jl95.rpc;

import java.util.UUID;

import jl95.lang.Awaitable;
import jl95.lang.variadic.*;
import jl95.net.Io;
import jl95.net.Receiver;
import jl95.net.ReceiversCollection;
import jl95.net.Sender;
import jl95.net.SendersCollection;
import jl95.rpc.util.Request;
import jl95.rpc.util.serdes.ResponseJsonSerdes;
import jl95.rpc.util.serdes.RequestJsonSerdes;
import jl95.rpc.util.Response;
import jl95.rpc.util.SerdesDefaults;

public abstract class Responder<A, R> {

    public static class StartWhenAlreadyRunningException extends RuntimeException {}
    public static class StopWhenNotRunningException      extends RuntimeException {}

    private final Receiver<Request>  receiver;
    private final Sender  <Response> sender;

    protected abstract A      readRequest  (byte[] serial);
    protected abstract byte[] writeResponse(R      object);

    private Responder(Receiver<Request>  receiver,
                      Sender  <Response> sender) {
        this.receiver = receiver;
        this.sender   = sender;
    }
    public  Responder(Io io) {

        this.receiver = ReceiversCollection.getBytesReceiver(io.getInputStream ()).extend(SerdesDefaults.stringFromBytes).extend(SerdesDefaults.jsonFromString).extend(RequestJsonSerdes ::fromJson);
        this.sender   = SendersCollection  .getBytesSender  (io.getOutputStream()).extend(SerdesDefaults.stringToBytes)  .extend(SerdesDefaults.jsonToString)  .extend(ResponseJsonSerdes::toJson);
    }

    synchronized public Awaitable<Void> start    (Function1<R, A> responseFunction) {

        if (isRunning()) throw new StartWhenAlreadyRunningException();
        return receiver.recvWhile(request -> {

            var requestPayloadObject = readRequest(request.payload);
            var responsePayloadObject   = responseFunction.apply(requestPayloadObject);
            var response       = new Response();
            response.id        = UUID.randomUUID();
            response.requestId = request.id;
            response.payload   = writeResponse(responsePayloadObject);
            sender.send(response);
            return true;
        });
    }
    synchronized public Awaitable<Void> stop     () {

        if (!isRunning()) throw new StopWhenNotRunningException();
        return receiver.recvStop();
    }
    synchronized public Boolean         isRunning() {

        return receiver.isReceiving();
    }
    public final <A2, R2> Responder<A2, R2> adapted(Function1<A2, A> argAdapter,
                                                    Function1<R, R2> reAdapter) {
        return new Responder<A2, R2>(receiver, sender) {

            @Override protected A2     readRequest  (byte[] serial) {
                return argAdapter.apply(Responder.this.readRequest(serial));
            }
            @Override protected byte[] writeResponse(R2     object) {
                return Responder.this.writeResponse(reAdapter.apply(object));
            }
        };
    }

}

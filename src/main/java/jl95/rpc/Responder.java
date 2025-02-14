package jl95.rpc;

import java.util.UUID;

import jl95.lang.Awaitable;
import jl95.lang.variadic.*;
import jl95.net.Ios;
import jl95.net.Receiver;
import jl95.net.ReceiverIf;
import jl95.net.Sender;
import jl95.net.SenderIf;
import jl95.rpc.util.Request;
import jl95.rpc.util.serdes.ResponseJsonSerdes;
import jl95.rpc.util.serdes.RequestJsonSerdes;
import jl95.rpc.util.Response;
import jl95.rpc.util.SerdesDefaults;

public class Responder implements ResponderIf<byte[], byte[]> {

    public static class StartWhenAlreadyRunningException extends RuntimeException {}
    public static class StopWhenNotRunningException      extends RuntimeException {}

    public static Responder fromSr(ReceiverIf<byte[]> receiver,
                                   SenderIf  <byte[]> sender) {
        return new Responder(receiver.adaptedReceiver(SerdesDefaults.stringFromBytes)
                                     .adaptedReceiver(SerdesDefaults.jsonFromString)
                                     .adaptedReceiver(RequestJsonSerdes::fromJson),
                             sender  .adaptedSender(SerdesDefaults.stringToBytes)
                                     .adaptedSender(SerdesDefaults.jsonToString)
                                     .adaptedSender(ResponseJsonSerdes::toJson));
    }
    public static Responder fromIo(Ios ios) {

        return fromSr(Receiver.of(ios.getInputStream ()),
                       Sender  .of(ios.getOutputStream()));
    }

    private final ReceiverIf<Request>  receiver;
    private final SenderIf  <Response> sender;

    private Responder(ReceiverIf<Request>  receiver,
                      SenderIf  <Response> sender) {
        this.receiver = receiver;
        this.sender   = sender;
    }

    @Override
    synchronized public Awaitable<Void> respondWhile(Function1<Tuple2<byte[], Boolean>, byte[]> responseFunction) {

        if (isRunning()) throw new StartWhenAlreadyRunningException();
        return receiver.recvWhile(request -> {

            var requestObject  = request.payload;
            var responseObject = responseFunction.apply(requestObject);
            var response       = new Response();
            response.id        = UUID.randomUUID();
            response.requestId = request.id;
            response.payload   = responseObject.a1;
            sender.send(response);
            return responseObject.a2;
        });
    }
    @Override
    synchronized public Awaitable<Void> stop() {

        if (!isRunning()) throw new StopWhenNotRunningException();
        return receiver.recvStop();
    }

    @Override
    public Boolean isRunning() {

        return receiver.isReceiving();
    }
}

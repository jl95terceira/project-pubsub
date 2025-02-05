package jl95.rpc;

import static jl95.lang.SuperPowers.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.Future;

import jl95.lang.variadic.*;
import jl95.net.BytesReceiver;
import jl95.net.BytesSender;
import jl95.net.Receiver;
import jl95.net.Sender;
import jl95.rpc.protocol.Response;
import jl95.rpc.serdes.ResponseJsonSerdes;
import jl95.rpc.serdes.RequestJsonSerdes;
import jl95.rpc.util.SerdesDefaults;

public abstract class GenericResponder<A, R> implements Responder<A, R> {

    public static class StartWhenAlreadyRunningException extends RuntimeException {}
    public static class StopWhenNotRunningException      extends RuntimeException {}

    private final Receiver<byte[]> receiver;
    private final Sender  <byte[]> sender;

    protected abstract A      readRequest  (byte[] serial);
    protected abstract byte[] writeResponse(R      object);

    public GenericResponder(InputStream  input,
                            OutputStream output) {

        this.receiver = new BytesReceiver(input);
        this.sender   = new BytesSender  (output);
    }

    @Override synchronized public void         start       (Function1<R, A> responseFunction) {

        if (isRunning()) throw new StartWhenAlreadyRunningException();
        receiver.recvWhile(serial -> {

            var request = RequestJsonSerdes.fromJson
                         (SerdesDefaults   .jsonFromString .call
                         (SerdesDefaults   .stringFromBytes.call(serial)));
            var requestPayloadObject = readRequest(request.payload);
            var responsePayloadObject   = responseFunction.call(requestPayloadObject);
            var response       = new Response();
            response.id        = UUID.randomUUID();
            response.requestId = request.id;
            response.payload   = writeResponse(responsePayloadObject);
            sender.send(SerdesDefaults .stringToBytes.call
                       (SerdesDefaults .jsonToString .call
                       (ResponseJsonSerdes.toJson(response))));
            return true;
        });
    }
    @Override synchronized public Future<Void> stop        () {

        if (!isRunning()) throw new StopWhenNotRunningException();
        return receiver.recvStop();
    }
    @Override synchronized public void         stopAwait   () { uncheck(() -> stop().get()); }
    @Override synchronized public Boolean      isRunning   () {
        return receiver.isReceiving();
    }
}

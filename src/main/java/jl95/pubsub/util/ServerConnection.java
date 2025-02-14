package jl95.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.net.*;
import jl95.net.ReceiverAdaptersCollection;
import jl95.net.SenderAdaptersCollections;
import jl95.pubsub.Message;
import jl95.pubsub.Subscription;
import jl95.pubsub.protocol.Publication;
import jl95.rpc.Responder;
import jl95.rpc.ResponderIf;
import jl95.rpc.ResponderAdaptersCollection;

public class ServerConnection {

    private final BlockingQueue<Message<Publication>>
                         queue          = new ArrayBlockingQueue<>(20);
    private      CompletableFuture<Void>
                         queueStopFuture;
    private      Boolean queueIsOn      = false;
    private      Boolean queueToStop    = false;

    public final Socket                             socket;
    public final Sender                             sender;
    public final SenderIf   <Message<Publication>>  pubSender;
    public final Receiver                           receiver;
    public final ReceiverIf<JsonValue>              jsonReceiver;
    public final Responder                          responder;
    public final ResponderIf<String, String>        clientRegResponder;
    public       Subscription                       subscription = (topic) -> false;

    public ServerConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.getLazySocketIos(socket);
        this.responder      = Responder.fromIo(ios);
        this.clientRegResponder = ResponderAdaptersCollection.getStringResponder(responder);
        this.receiver       = Receiver.of(ios.getInputStream());
        this.jsonReceiver   = ReceiverAdaptersCollection.getJsonReceiver(receiver);
        this.sender         = Sender  .of(ios.getOutputStream());
        this.pubSender      = SenderAdaptersCollections .getJsonSender  (sender).adaptedSender(SerdesDefaults.pubMsgToJson);
    }

    synchronized
    public final void            startQueue    () {
        if (queueIsOn) { throw new IllegalStateException(); };
        queueToStop     = false;
        queueStopFuture = new CompletableFuture<>();
        new Thread(() -> {
            while (!queueToStop) {
                var pub = uncheck(() -> queue.poll(125L, TimeUnit.MILLISECONDS));
                if (pub == null) continue;
                pubSender.send(pub);
            }
            queueIsOn = false;
            queueStopFuture.complete(null);
        }).start();
        queueIsOn = true;
    }
    public final Awaitable<Void> stopQueue     () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return Awaitable.of(queueStopFuture);
    }
    public final Boolean         isQueueRunning() { return queueIsOn; }
    public final void            pub           (Message<Publication> pubMsg) {

        queue.add(pubMsg);
    }
}

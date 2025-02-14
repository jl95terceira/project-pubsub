package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.net.sr.Receiver;
import jl95.net.sr.ReceiverAdaptersCollection;
import jl95.net.sr.Sender;
import jl95.net.sr.SenderAdaptersCollections;
import jl95.net.sr.Ios;
import jl95.net.sr.ReceiverIf;
import jl95.net.sr.SenderIf;
import jl95.net.pubsub.Message;
import jl95.net.pubsub.Subscription;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.rpc.Responder;
import jl95.net.rpc.ResponderIf;
import jl95.net.rpc.ResponderAdaptersCollection;

public class ServerConnection {

    private final BlockingQueue<Message<Publication>>
                         queue          = new ArrayBlockingQueue<>(20);
    private      CompletableFuture<Void>
                         queueStopFuture;
    private      Boolean queueIsOn      = false;
    private      Boolean queueToStop    = false;

    public final Socket                             socket;
    public final Sender sender;
    public final SenderIf<Message<Publication>> pubSender;
    public final Receiver receiver;
    public final ReceiverIf<JsonValue> jsonReceiver;
    public final Responder                          responder;
    public final ResponderIf<String, String>        clientRegResponder;
    public       Subscription                       subscription = (topic) -> false;

    public ServerConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        this.responder      = Responder.fromIo(ios);
        this.clientRegResponder = ResponderAdaptersCollection.asStringResponder(responder);
        this.receiver       = Receiver.of(ios.getInputStream());
        this.jsonReceiver   = ReceiverAdaptersCollection.asJsonReceiver(receiver);
        this.sender         = Sender  .of(ios.getOutputStream());
        this.pubSender      = SenderAdaptersCollections .asJsonSender(sender).adaptedSender(SerdesDefaults.pubMsgToJson);
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

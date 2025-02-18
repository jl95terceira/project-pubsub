package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Method1;
import jl95.net.io.Receiver;
import jl95.net.io.collections.ReceiverAdaptersCollection;
import jl95.net.io.Sender;
import jl95.net.io.collections.SenderAdaptersCollections;
import jl95.net.io.Ios;
import jl95.net.io.ReceiverIf;
import jl95.net.io.SenderIf;
import jl95.net.pubsub.Subscription;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.serdes.MessageSwitchedDeserializer;
import jl95.net.rpc.Responder;
import jl95.net.rpc.ResponderIf;
import jl95.net.rpc.collections.ResponderAdaptersCollection;
import jl95.serdes.StringFromJson;

public class BrokerConnection {

    private final BlockingQueue<Message<Publication>>
                         queue          = new ArrayBlockingQueue<>(20);
    private      CompletableFuture<Void>
                         queueStopFuture;
    private      Boolean queueIsOn      = false;
    private      Boolean queueToStop    = false;

    public final Socket                         socket;
    public final SenderIf<Message<Publication>> pubSender;
    public final ReceiverIf<JsonValue>          jsonReceiver;
    public final ResponderIf<JsonValue, Void>   jsonResponder;
    public       Subscription                   subscription = (topic) -> false;

    public BrokerConnection(Socket          socket,
                            Method1<String> memberIdCb) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        var sender          = Sender   .of    (ios.getOutputStream());
        var receiver        = Receiver .of    (ios.getInputStream());
        var responder       = Responder.fromIo(ios);
        this.pubSender      = SenderAdaptersCollections  .asJsonSender       (sender).adaptedSender(SerdesDefaults.pubMsgToJson);
        this.jsonReceiver   = ReceiverAdaptersCollection .asJsonReceiver     (receiver);
        this.jsonResponder  = ResponderAdaptersCollection.asJsonPostResponder(responder);
        // receive client ID and put in connections map
        var stringResponder = ResponderAdaptersCollection.asStringPostResponder(responder);
        stringResponder.respondOnce(clientId -> {
            memberIdCb.accept(clientId);
            return null;
        });
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

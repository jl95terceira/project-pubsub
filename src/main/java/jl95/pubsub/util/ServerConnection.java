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
import jl95.net.collections.IosSuppliersCollection;
import jl95.net.collections.ReceiversCollection;
import jl95.net.collections.SendersCollection;
import jl95.pubsub.Subscription;
import jl95.pubsub.protocol.Publication;

public class ServerConnection {

    private final BlockingQueue<Message<Publication>>
                                    queue          = new ArrayBlockingQueue<>(20);
    private      CompletableFuture<Void>
                                    queueStopFuture;
    private      Boolean            queueIsOn      = false;
    private      Boolean            queueToStop    = false;

    public final Socket                         socket;
    public final Sender<Message<Publication>>   pubSender;
    public final Receiver<JsonValue>            jsonReceiver;
    public       Subscription                   subscription = (topic) -> false;

    public ServerConnection(Socket socket) {
        this.socket        = socket;
        this.jsonReceiver  = ReceiversCollection.getJsonReceiver(IosSuppliersCollection.getSocketIos(socket));
        this.pubSender     = SendersCollection.getJsonSender  (IosSuppliersCollection.getSocketIos(socket)).adapted(SerdesDefaults.pubMsgToJson);
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

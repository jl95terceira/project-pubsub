package jl95.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jl95.net.util.ReceiverBySocket;
import jl95.net.util.SenderBySocket;
import jl95.pubsub.protocol.Message;
import jl95.net.*;
import jl95.pubsub.Subscription;
import jl95.pubsub.protocol.Publication;

public class Connection {

    private final BlockingQueue<Message<Publication>>
                                    queue          = new ArrayBlockingQueue<>(20);
    private      CompletableFuture<Void>
                                    queueStopFuture;
    private      Boolean            queueIsOn      = false;
    private      Boolean            queueToStop    = false;

    public final Socket         socket;
    public final Sender<Message<Publication>>
                                pubSender;
    public final JsonReceiver   jsonReceiver;
    public       Subscription   subscription   = (topic) -> false;

    public Connection(Socket socket) {
        this.socket       = socket;
        this.jsonReceiver = ReceiverBySocket.get(socket, JsonReceiver::new);
        this.pubSender    = SenderBySocket  .get(socket, JsonSender  ::new)
                                            .extend(SerdesDefaults.pubMsgToJson);
    }

    synchronized
    public final void         startQueue    () {
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
    public final Future<Void> stopQueue     () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return queueStopFuture;
    }
    public final void         stopQueueAwait() {

        uncheck(() -> stopQueue().get());
    }
    public final Boolean      isQueueRunning() { return queueIsOn; }
    public final void         pub           (Message<Publication> pubMsg) {

        queue.add(pubMsg);
    }
}

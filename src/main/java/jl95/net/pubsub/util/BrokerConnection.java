package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
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
import jl95.net.rpc.Responder;
import jl95.net.rpc.ResponderIf;
import jl95.net.rpc.collections.ResponderAdaptersCollection;

public class BrokerConnection {

    private final BlockingQueue<Message<Publication>>
                         queue          = new ArrayBlockingQueue<>(20);
    private      CompletableFuture<Void>
                         queueStopFuture;
    private      Boolean queueIsOn      = false;
    private      Boolean queueToStop    = false;

    private final Socket                         socket;
    private final SenderIf<Message<Publication>> pubSender;
    private final ReceiverIf<JsonValue>          jsonReceiver;
    private       Subscription                   subscription = (topic) -> false;

    public BrokerConnection(Socket          socket,
                            Method1<String> memberIdCb) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        var sender          = Sender   .of    (ios.getOutputStream());
        var receiver        = Receiver .of    (ios.getInputStream());
        var responder       = Responder.fromIo(ios);
        this.pubSender      = SenderAdaptersCollections  .asJsonSender       (sender).adaptedSender(SerdesDefaults.pubMsgToJson);
        this.jsonReceiver   = ReceiverAdaptersCollection .asJsonReceiver     (receiver);
        // receive client ID and put in connections map
        var stringResponder = ResponderAdaptersCollection.asStringPostResponder(responder);
        stringResponder.respondOnce(clientId -> {
            memberIdCb.accept(clientId);
            return null;
        });
    }

    synchronized public final void            startPubQueue    () {
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
    synchronized public final Awaitable<Void> stopPubQueue     () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return Awaitable.of(queueStopFuture);
    }
    synchronized public final Boolean         isPubQueueRunning() { return queueIsOn; }

    public final void           startRespond    (Function1<Boolean, JsonValue> msgJsonCb,
                                                 ReceiverIf.RecvOptions        options) {

        jsonReceiver.recvWhile(msgJsonCb, options);
    }
    public final void           stopRespond     () {

        jsonReceiver.recvStop();
    }
    public final Boolean        isResponding    () {

        return jsonReceiver.isReceiving();
    }
    public final void           pub             (Message<Publication>          pubMsg) {

        if (!isPubQueueRunning()) {
            startPubQueue();

        }
        queue.add(pubMsg);
    }
    public final Socket         getSocket       () { return socket; }
    public final Subscription   getSubscription () { return subscription; }
    public final void           setSubscription (Subscription s) { subscription = s; }
}

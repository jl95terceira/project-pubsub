package jl95.rpc;

import static jl95.lang.SuperPowers.*;

import java.util.concurrent.CompletableFuture;

import jl95.rpc.util.Defaults;
import jl95.net.CloseableIosSupplier;
import jl95.rpc.util.Util;

public class Test {

    CloseableIosSupplier ioAsServer;
    CloseableIosSupplier ioAsClient;
    Requester<String, String> requester;
    Responder<String, String> responder;

    @org.junit.Before
    public void setUp() throws Exception {
        var requesterFuture = CompletableFuture.supplyAsync(() -> {
            ioAsServer = Util.getIoAsServer(jl95.net.util.Defaults.serverAddr);
            return RequestersCollection.getStringRequester(ioAsServer);
        }, (task) -> new Thread(task).start());
        sleep(50);
        var responderFuture = CompletableFuture.supplyAsync(() -> {
            ioAsClient = Util.getIoAsClient(jl95.net.util.Defaults.serverAddr);
            return RespondersCollection.getStringResponser(ioAsClient);
        }, (task) -> new Thread(task).start());
        requester = requesterFuture.get();
        responder = responderFuture.get();
    }
    @org.junit.After
    public void tearDown() {
        if (responder.isRunning()) responder.stop().await();
        if (ioAsClient != null) ioAsClient.close();
        if (ioAsServer != null) ioAsServer.close();
    }

    @org.junit.Test
    public void testStartStop() {
        responder.start(self::apply).await();
        responder.stop ()           .await();
        responder.start(self::apply).await();
        responder.stop ()           .await();
    }
    @org.junit.Test
    public void test() {
        responder.start(msg -> "hello, " + msg).await();
        org.junit.Assert.assertEquals("hello, world", requester.apply("world"));
    }
    @org.junit.Test
    public void test2() { // to confirm that the server socket is closed correctly (in tearDown) - otherwise, an address binding error will happen
        responder.start(msg -> "greetings, " + msg).await();
        org.junit.Assert.assertEquals("greetings, universe", requester.apply("universe"));
    }
    @org.junit.Test
    public void testTimeout() {
        responder.start(self::apply).await();
        org.junit.Assert.assertEquals("first", requester.apply("first"));
        responder.stop().await();
        responder.start(msg -> {
            sleep(Defaults.responseTimeoutMs + 1000);
            return "";
        }).await();
        try {
            requester.apply("second (to time out)");
            org.junit.Assert.fail("response timeout exception must be raised");
        }
        catch (Requester.ResponseTimeoutException ex) {/* as expected */}
        responder.stop ()           .await();
        responder.start(self::apply).await();
        org.junit.Assert.assertEquals("third", requester.apply("third"));
    }
}

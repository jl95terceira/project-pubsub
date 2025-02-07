package jl95.rpc;

import static jl95.lang.SuperPowers.*;

import java.util.concurrent.CompletableFuture;

import jl95.rpc.impl.StringRequesterFactory;
import jl95.rpc.impl.StringResponderFactory;
import jl95.rpc.util.Defaults;
import jl95.rpc.util.CloseableIo;
import jl95.rpc.util.Util;

public class Test {

    CloseableIo ioAsServer;
    CloseableIo ioAsClient;
    RequesterIf<String, String> requester;
    ResponderIf<String, String> responder;

    @org.junit.Before
    public void setUp() throws Exception {
        System.out.println("Setup");
        var requesterFuture = CompletableFuture.supplyAsync(() -> {
            ioAsServer = Util.getIoAsServer(jl95.net.util.Defaults.serverAddr);
            return StringRequesterFactory.get(ioAsServer, Requester.SendOptions.defaults());
        }, (task) -> new Thread(task).start());
        sleep(50);
        var responderFuture = CompletableFuture.supplyAsync(() -> {
            ioAsClient = Util.getIoAsClient(jl95.net.util.Defaults.serverAddr);
            return StringResponderFactory.get(ioAsClient);
        }, (task) -> new Thread(task).start());
        requester = requesterFuture.get();
        responder = responderFuture.get();
    }
    @org.junit.After
    public void tearDown() {
        System.out.println("Teardown");
        if (ioAsClient != null) ioAsClient.close();
        System.out.println("Client closed");
        if (ioAsServer != null) ioAsServer.close();
        System.out.println("Server closed");
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
        responder.start(msg -> {
            sleep(Defaults.responseTimeoutMs + 1000);
            return "";
        });
        try {
            requester.apply("whatever");
            org.junit.Assert.fail("response timeout exception must be raised");
        }
        catch (Requester.ResponseTimeoutException ex) {/* as expected */}
        responder.stop ()           .await();
        responder.start(self::apply).await();
        org.junit.Assert.assertEquals("test", requester.apply("test"));
    }
}
